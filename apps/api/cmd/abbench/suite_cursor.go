package main

import (
	domainfeed "GCFeed/internal/domain/feed"
	domainvideo "GCFeed/internal/domain/video"
	infrafeed "GCFeed/internal/infra/persistence/feed"
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

type CursorDepthResult struct {
	Depth           int          `json:"depth"`
	AnchorVideoID   int64        `json:"anchor_video_id"`
	OffsetStats     LatencyStats `json:"offset_stats"`
	CursorStats     LatencyStats `json:"cursor_stats"`
	SpeedupAvg      float64      `json:"speedup_avg"`
	SpeedupP95      float64      `json:"speedup_p95"`
	ExplainOffset   string       `json:"explain_analyze_offset"`
	ExplainCursor   string       `json:"explain_analyze_cursor"`
}

type CursorConcurrencyGroup struct {
	Concurrency      int              `json:"concurrency"`
	Method           string           `json:"method"`
	Stats            LatencyStats     `json:"stats"`
	MySQLStatusDelta map[string]int64 `json:"mysql_status_delta"`
}

type CursorSuiteResult struct {
	Timestamp         string                   `json:"timestamp"`
	VideoTotalCount   int64                    `json:"video_total_count"`
	DepthSweep        []CursorDepthResult      `json:"depth_sweep"`
	ConcurrentReplay  []CursorConcurrencyGroup `json:"concurrent_replay"`
}

type anchorPoint struct {
	Depth       int
	VideoID     int64
	PublishedAt time.Time
}

func runCursorSuite(env *BenchEnv) error {
	ctx := context.Background()
	fmt.Println("=== Running Suite 1: Composite Cursor vs OFFSET ===")

	var totalVideos int64
	if err := env.SQLDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM video WHERE status = ? AND published_at IS NOT NULL", domainvideo.StatusPublished).Scan(&totalVideos); err != nil {
		return err
	}

	repo := infrafeed.New(env.GormDB)
	depths := []int{100, 1000, 5000, 10000, 20000, 30000, 45000}
	sweep := make([]CursorDepthResult, 0, len(depths))

	for _, depth := range depths {
		anchor, err := fetchAnchorAtDepth(ctx, env, depth)
		if err != nil {
			return fmt.Errorf("fetch anchor depth=%d: %w", depth, err)
		}

		// Warmup 2 times each
		for range 2 {
			_, _ = listTimelineByOffset(ctx, env, depth, 11)
			_, _ = repo.ListTimelinePage(ctx, &domainfeed.TimelineCursor{
				PublishedAt: anchor.PublishedAt,
				VideoID:     anchor.VideoID,
			}, 11)
		}

		const iters = 25
		offsetDurs := make([]time.Duration, 0, iters)
		var offsetErr int64
		startOffset := time.Now()
		for range iters {
			t0 := time.Now()
			items, qErr := listTimelineByOffset(ctx, env, depth, 11)
			offsetDurs = append(offsetDurs, time.Since(t0))
			if qErr != nil || len(items) == 0 {
				offsetErr++
			}
		}
		offsetStats := calcLatencyStats(offsetDurs, time.Since(startOffset), offsetErr)

		cursorDurs := make([]time.Duration, 0, iters)
		var cursorErr int64
		startCursor := time.Now()
		cursor := &domainfeed.TimelineCursor{PublishedAt: anchor.PublishedAt, VideoID: anchor.VideoID}
		for range iters {
			t0 := time.Now()
			items, qErr := repo.ListTimelinePage(ctx, cursor, 11)
			cursorDurs = append(cursorDurs, time.Since(t0))
			if qErr != nil || len(items) == 0 {
				cursorErr++
			}
		}
		cursorStats := calcLatencyStats(cursorDurs, time.Since(startCursor), cursorErr)

		expOffset := explainAnalyzeOffset(ctx, env, depth)
		expCursor := explainAnalyzeCursor(ctx, env, anchor)

		speedupAvg := 0.0
		if cursorStats.AvgMS > 0 {
			speedupAvg = round2(offsetStats.AvgMS / cursorStats.AvgMS)
		}
		speedupP95 := 0.0
		if cursorStats.P95MS > 0 {
			speedupP95 = round2(offsetStats.P95MS / cursorStats.P95MS)
		}

		fmt.Printf("  depth=%5d | OFFSET avg=%.3fms p95=%.3fms | CURSOR avg=%.3fms p95=%.3fms | speedup=%.2fx\n",
			depth, offsetStats.AvgMS, offsetStats.P95MS, cursorStats.AvgMS, cursorStats.P95MS, speedupAvg)

		sweep = append(sweep, CursorDepthResult{
			Depth:         depth,
			AnchorVideoID: anchor.VideoID,
			OffsetStats:   offsetStats,
			CursorStats:   cursorStats,
			SpeedupAvg:    speedupAvg,
			SpeedupP95:    speedupP95,
			ExplainOffset: expOffset,
			ExplainCursor: expCursor,
		})
	}

	// Sub-experiment 1B: concurrent deep-page queries (depths 20,000 ~ 44,800) bypassing page cache
	anchors := make([]anchorPoint, 0, 200)
	for depth := 20000; depth <= 44800; depth += 125 {
		a, err := fetchAnchorAtDepth(ctx, env, depth)
		if err != nil {
			return err
		}
		anchors = append(anchors, a)
	}

	var concurrentResults []CursorConcurrencyGroup
	for _, conc := range []int{20, 50} {
		for _, method := range []string{"offset", "cursor"} {
			beforeStatus := snapshotMySQLStatus(env.SQLDB)
			stats := runConcurrentDeepPaging(ctx, env, repo, anchors, conc, 15*time.Second, method)
			afterStatus := snapshotMySQLStatus(env.SQLDB)
			delta := diffMySQLStatus(beforeStatus, afterStatus)

			fmt.Printf("  concurrent=%2d method=%-6s | QPS=%7.2f avg=%.3fms p95=%.3fms p99=%.3fms err=%d Handler_read_next=%d\n",
				conc, method, stats.QPS, stats.AvgMS, stats.P95MS, stats.P99MS, stats.Errors, delta["Handler_read_next"])

			concurrentResults = append(concurrentResults, CursorConcurrencyGroup{
				Concurrency:      conc,
				Method:           method,
				Stats:            stats,
				MySQLStatusDelta: delta,
			})
		}
	}

	res := CursorSuiteResult{
		Timestamp:        time.Now().Format(time.RFC3339),
		VideoTotalCount:  totalVideos,
		DepthSweep:       sweep,
		ConcurrentReplay: concurrentResults,
	}
	return writeJSON(env.OutDir, "suite1_cursor_vs_offset.json", res)
}

func fetchAnchorAtDepth(ctx context.Context, env *BenchEnv, depth int) (anchorPoint, error) {
	var a anchorPoint
	a.Depth = depth
	row := env.SQLDB.QueryRowContext(
		ctx,
		"SELECT id, published_at FROM video WHERE status = ? AND published_at IS NOT NULL ORDER BY published_at DESC, id DESC LIMIT ?, 1",
		domainvideo.StatusPublished,
		depth-1,
	)
	if err := row.Scan(&a.VideoID, &a.PublishedAt); err != nil {
		return a, err
	}
	return a, nil
}

func listTimelineByOffset(ctx context.Context, env *BenchEnv, offset, limit int) ([]domainfeed.FeedPageItem, error) {
	rows, err := env.SQLDB.QueryContext(
		ctx,
		"SELECT id AS video_id, author_id, published_at FROM video WHERE status = ? AND published_at IS NOT NULL ORDER BY published_at DESC, id DESC LIMIT ?, ?",
		domainvideo.StatusPublished,
		offset,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]domainfeed.FeedPageItem, 0, limit)
	for rows.Next() {
		var item domainfeed.FeedPageItem
		if err := rows.Scan(&item.VideoID, &item.AuthorID, &item.PublishedAt); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func explainAnalyzeOffset(ctx context.Context, env *BenchEnv, depth int) string {
	var out string
	_ = env.SQLDB.QueryRowContext(
		ctx,
		fmt.Sprintf("EXPLAIN ANALYZE SELECT id, author_id, published_at FROM video WHERE status = %d AND published_at IS NOT NULL ORDER BY published_at DESC, id DESC LIMIT %d, 11", domainvideo.StatusPublished, depth),
	).Scan(&out)
	return out
}

func explainAnalyzeCursor(ctx context.Context, env *BenchEnv, anchor anchorPoint) string {
	var out string
	ts := anchor.PublishedAt.Format("2006-01-02 15:04:05.000000")
	query := fmt.Sprintf(
		"EXPLAIN ANALYZE SELECT id, author_id, published_at FROM video WHERE status = %d AND published_at IS NOT NULL AND (published_at < '%s' OR (published_at = '%s' AND id < %d)) ORDER BY published_at DESC, id DESC LIMIT 11",
		domainvideo.StatusPublished, ts, ts, anchor.VideoID,
	)
	_ = env.SQLDB.QueryRowContext(ctx, query).Scan(&out)
	return out
}

func runConcurrentDeepPaging(ctx context.Context, env *BenchEnv, repo *infrafeed.Repository, anchors []anchorPoint, concurrency int, duration time.Duration, method string) LatencyStats {
	runCtx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()

	var wg sync.WaitGroup
	var errCount atomic.Int64
	perWorkerDurs := make([][]time.Duration, concurrency)

	start := time.Now()
	for w := range concurrency {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)*997))
			local := make([]time.Duration, 0, 256)
			for runCtx.Err() == nil {
				anchor := anchors[rng.Intn(len(anchors))]
				t0 := time.Now()
				var qErr error
				if method == "offset" {
					_, qErr = listTimelineByOffset(runCtx, env, anchor.Depth, 11)
				} else {
					_, qErr = repo.ListTimelinePage(runCtx, &domainfeed.TimelineCursor{
						PublishedAt: anchor.PublishedAt,
						VideoID:     anchor.VideoID,
					}, 11)
				}
				elapsed := time.Since(t0)
				if runCtx.Err() != nil {
					break
				}
				if qErr != nil {
					errCount.Add(1)
				} else {
					local = append(local, elapsed)
				}
			}
			perWorkerDurs[workerID] = local
		}(w)
	}
	wg.Wait()
	totalElapsed := time.Since(start)

	allDurs := make([]time.Duration, 0, 4096)
	for _, list := range perWorkerDurs {
		allDurs = append(allDurs, list...)
	}
	return calcLatencyStats(allDurs, totalElapsed, errCount.Load())
}
