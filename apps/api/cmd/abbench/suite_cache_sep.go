package main

import (
	applicationfeed "GCFeed/internal/application/feed"
	domainfeed "GCFeed/internal/domain/feed"
	domaininteraction "GCFeed/internal/domain/interaction"
	infracache "GCFeed/internal/infra/cache"
	infrafeed "GCFeed/internal/infra/persistence/feed"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

type MemoryUsageResult struct {
	Mode              string  `json:"mode"`
	KeyCount          int64   `json:"key_count"`
	TotalKeyBytes     int64   `json:"total_key_bytes"`
	RedisUsedMemDelta int64   `json:"redis_used_memory_delta_bytes"`
	BytesPerLogicalPg float64 `json:"bytes_per_logical_page"`
}

type MixedReadWriteResult struct {
	Mode             string           `json:"mode"`
	ReadStats        LatencyStats     `json:"read_stats"`
	WriteOps         int64            `json:"write_ops"`
	PageHitCount     int64            `json:"page_hit_count"`
	PageMissCount    int64            `json:"page_miss_count"`
	DetailHitCount   int64            `json:"detail_hit_count"`
	DetailMissCount  int64            `json:"detail_miss_count"`
	EffectiveHitRate float64          `json:"effective_hit_rate_pct"`
	MySQLStatusDelta map[string]int64 `json:"mysql_status_delta"`
}

type PersonalizedReuseResult struct {
	Mode             string           `json:"mode"`
	ReadStats        LatencyStats     `json:"read_stats"`
	PageHitRatePct   float64          `json:"page_hit_rate_pct"`
	CardHitRatePct   float64          `json:"card_hit_rate_pct"`
	StatHitRatePct   float64          `json:"stat_hit_rate_pct"`
	MySQLStatusDelta map[string]int64 `json:"mysql_status_delta"`
}

type CacheSepSuiteResult struct {
	Timestamp         string                    `json:"timestamp"`
	MemoryCompare     []MemoryUsageResult       `json:"memory_compare"`
	MixedReadWrite    []MixedReadWriteResult    `json:"mixed_read_write"`
	PersonalizedReuse []PersonalizedReuseResult `json:"personalized_reuse"`
}

func runCacheSepSuite(env *BenchEnv) error {
	ctx := context.Background()
	fmt.Println("=== Running Suite 2: ID-Detail Cache Separation vs Monolithic Page Cache ===")

	repo := infrafeed.New(env.GormDB)
	feedCache := infracache.NewFeedCache(env.Redis)

	// Load top 500 latest videos as hot working set
	var videoIDs []int64
	rows, err := env.SQLDB.QueryContext(ctx, "SELECT id FROM video WHERE status = 2 AND published_at IS NOT NULL ORDER BY published_at DESC, id DESC LIMIT 500")
	if err != nil {
		return err
	}
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		videoIDs = append(videoIDs, id)
	}
	rows.Close()
	if len(videoIDs) < 200 {
		return fmt.Errorf("expected at least 200 videos, got %d", len(videoIDs))
	}

	cards, err := repo.BatchGetFeedCards(ctx, videoIDs)
	if err != nil {
		return err
	}
	stats, err := repo.BatchGetFeedStats(ctx, videoIDs)
	if err != nil {
		return err
	}

	// Build 200 logical pages (10 items/page):
	// First 20 pages = sequential timeline pages; remaining 180 pages = personalized combinations from hot 500 pool.
	const numPages = 200
	logicalPages := make([][]*domainfeed.FeedPageItem, numPages)
	videoToPages := make(map[int64][]int)
	rng := rand.New(rand.NewSource(42))

	for p := range numPages {
		pageItems := make([]*domainfeed.FeedPageItem, 0, 10)
		used := map[int64]bool{}
		for j := range 10 {
			var vid int64
			if p < 20 {
				vid = videoIDs[p*10+j]
			} else {
				// Hot-biased selection from top 500 videos (front 100 videos hotter)
				for {
					idx := rng.Intn(len(videoIDs))
					if rng.Intn(10) < 7 {
						idx = rng.Intn(min(150, len(videoIDs)))
					}
					if !used[videoIDs[idx]] {
						vid = videoIDs[idx]
						break
					}
				}
			}
			used[vid] = true
			card := cards[vid]
			pageItems = append(pageItems, &domainfeed.FeedPageItem{
				VideoID:     vid,
				AuthorID:    card.AuthorID,
				PublishedAt: card.PublishedAt,
			})
			videoToPages[vid] = append(videoToPages[vid], p)
		}
		logicalPages[p] = pageItems
	}

	// --- Sub-experiment 2A: Redis Memory Footprint ---
	memResults := make([]MemoryUsageResult, 0, 2)

	// Group A: Monolithic full FeedItem[] page JSON
	if err := env.Redis.FlushDB(ctx).Err(); err != nil {
		return err
	}
	memBeforeMono := snapshotRedisMemory(ctx, env.Redis)
	var monoBytes int64
	for p := range numPages {
		fullItems := assembleInMem(logicalPages[p], cards, stats)
		raw, err := json.Marshal(fullItems)
		if err != nil {
			return err
		}
		key := fmt.Sprintf("bench:mono:page:%d", p)
		if err := env.Redis.Set(ctx, key, raw, 15*time.Minute).Err(); err != nil {
			return err
		}
		u, _ := env.Redis.MemoryUsage(ctx, key).Result()
		monoBytes += u
	}
	memAfterMono := snapshotRedisMemory(ctx, env.Redis)
	monoKeyCnt, _ := env.Redis.DBSize(ctx).Result()
	memResults = append(memResults, MemoryUsageResult{
		Mode:              "monolithic_page_cache",
		KeyCount:          monoKeyCnt,
		TotalKeyBytes:     monoBytes,
		RedisUsedMemDelta: max(0, memAfterMono-memBeforeMono),
		BytesPerLogicalPg: round2(float64(monoBytes) / float64(numPages)),
	})

	// Group B: ID-Detail separation (200 lightweight FeedPage + 500 FeedCard + 500 FeedStat)
	if err := env.Redis.FlushDB(ctx).Err(); err != nil {
		return err
	}
	memBeforeSep := snapshotRedisMemory(ctx, env.Redis)
	for p := range numPages {
		page := &applicationfeed.FeedPage{
			Scene: domainfeed.SceneTimeline,
			Items: logicalPages[p],
		}
		if err := feedCache.SetPage(ctx, fmt.Sprintf("bench:sep:page:%d", p), page, 15*time.Minute); err != nil {
			return err
		}
	}
	if err := feedCache.SetCards(ctx, cards, 15*time.Minute); err != nil {
		return err
	}
	if err := feedCache.SetStats(ctx, stats, 15*time.Minute); err != nil {
		return err
	}
	sepBytes, sepKeyCnt, err := sumAllKeyMemory(ctx, env.Redis)
	if err != nil {
		return err
	}
	memAfterSep := snapshotRedisMemory(ctx, env.Redis)
	memResults = append(memResults, MemoryUsageResult{
		Mode:              "id_detail_separated_cache",
		KeyCount:          sepKeyCnt,
		TotalKeyBytes:     sepBytes,
		RedisUsedMemDelta: max(0, memAfterSep-memBeforeSep),
		BytesPerLogicalPg: round2(float64(sepBytes) / float64(numPages)),
	})
	fmt.Printf("  2A Memory: Mono=%d bytes (%d keys) vs Separated=%d bytes (%d keys), saved=%.2f%%\n",
		monoBytes, monoKeyCnt, sepBytes, sepKeyCnt, (1.0-float64(sepBytes)/float64(monoBytes))*100)

	// --- Sub-experiment 2B: Mixed Read (50) + Like-Write (10) over 20 hot pages (15s) ---
	mixedResults := []MixedReadWriteResult{
		runMixedReadWriteMono(ctx, env, repo, logicalPages[:20], videoIDs[:200], videoToPages, cards, stats),
		runMixedReadWriteSep(ctx, env, repo, feedCache, logicalPages[:20], videoIDs[:200], cards, stats),
	}
	for _, r := range mixedResults {
		fmt.Printf("  2B Mixed(%s): ReadQPS=%.2f p95=%.3fms hitRate=%.2f%% MySQL_Com_select=%d\n",
			r.Mode, r.ReadStats.QPS, r.ReadStats.P95MS, r.EffectiveHitRate, r.MySQLStatusDelta["Com_select"])
	}

	// --- Sub-experiment 2C: Personalized Multi-User Feed Assembly (no page reuse, 50 readers 15s) ---
	// Simulates Following / Recommend where every user request produces a personalized ID list from the 500 hot pool.
	// Mono caches per-user-request monolithic page (misses on unique user/cursor, hits DB for cards+stats on miss);
	// ID-Detail caches shared Card + Stat across all users.
	persResults := []PersonalizedReuseResult{
		runPersonalizedMono(ctx, env, repo, videoIDs, cards, stats),
		runPersonalizedSep(ctx, env, repo, feedCache, videoIDs, cards, stats),
	}
	for _, r := range persResults {
		fmt.Printf("  2C Personalized(%s): ReadQPS=%.2f p95=%.3fms cardHit=%.2f%% statHit=%.2f%% MySQL_Com_select=%d\n",
			r.Mode, r.ReadStats.QPS, r.ReadStats.P95MS, r.CardHitRatePct, r.StatHitRatePct, r.MySQLStatusDelta["Com_select"])
	}

	return writeJSON(env.OutDir, "suite2_cache_separation.json", CacheSepSuiteResult{
		Timestamp:         time.Now().Format(time.RFC3339),
		MemoryCompare:     memResults,
		MixedReadWrite:    mixedResults,
		PersonalizedReuse: persResults,
	})
}

func assembleInMem(pageItems []*domainfeed.FeedPageItem, cards map[int64]*domainfeed.FeedCard, stats map[int64]*domainfeed.FeedStat) []*domainfeed.FeedItem {
	out := make([]*domainfeed.FeedItem, 0, len(pageItems))
	for _, pi := range pageItems {
		c := cards[pi.VideoID]
		st := stats[pi.VideoID]
		if c == nil {
			continue
		}
		if st == nil {
			st = &domainfeed.FeedStat{VideoID: pi.VideoID}
		}
		out = append(out, domainfeed.RestoreFeedItem(
			c.VideoID, c.AuthorID, c.AuthorNickname, c.AuthorAvatarURL,
			c.Title, c.Description, c.MediaURL, c.CoverURL,
			st.LikeCount, st.CommentCount, st.FavoriteCount, pi.PublishedAt,
		))
	}
	return out
}

func sumAllKeyMemory(ctx context.Context, rdb *redis.Client) (int64, int64, error) {
	var totalBytes, totalKeys int64
	var cursor uint64
	for {
		keys, nextCursor, err := rdb.Scan(ctx, cursor, "*", 500).Result()
		if err != nil {
			return 0, 0, err
		}
		if len(keys) > 0 {
			pipe := rdb.Pipeline()
			cmds := make([]*redis.IntCmd, 0, len(keys))
			for _, k := range keys {
				cmds = append(cmds, pipe.MemoryUsage(ctx, k))
			}
			_, _ = pipe.Exec(ctx)
			for _, cmd := range cmds {
				if u, err := cmd.Result(); err == nil {
					totalBytes += u
				}
			}
			totalKeys += int64(len(keys))
		}
		cursor = nextCursor
		if cursor == 0 {
			break
		}
	}
	return totalBytes, totalKeys, nil
}

func runMixedReadWriteMono(
	ctx context.Context,
	env *BenchEnv,
	repo *infrafeed.Repository,
	hotPages [][]*domainfeed.FeedPageItem,
	hotVideoIDs []int64,
	videoToPages map[int64][]int,
	cards map[int64]*domainfeed.FeedCard,
	stats map[int64]*domainfeed.FeedStat,
) MixedReadWriteResult {
	_ = env.Redis.FlushDB(ctx).Err()
	for p := range hotPages {
		raw, _ := json.Marshal(assembleInMem(hotPages[p], cards, stats))
		_ = env.Redis.Set(ctx, fmt.Sprintf("bench:mono:hot:%d", p), raw, 5*time.Minute).Err()
	}

	before := snapshotMySQLStatus(env.SQLDB)
	runCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	var readErrs, writeOps, pageHits, pageMisses atomic.Int64
	const readers = 50
	const writers = 10
	perReaderDurs := make([][]time.Duration, readers)
	var wg sync.WaitGroup

	start := time.Now()
	for r := range readers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)*31))
			local := make([]time.Duration, 0, 512)
			for runCtx.Err() == nil {
				p := rng.Intn(len(hotPages))
				key := fmt.Sprintf("bench:mono:hot:%d", p)
				t0 := time.Now()
				raw, err := env.Redis.Get(runCtx, key).Bytes()
				if err == nil {
					var items []*domainfeed.FeedItem
					if json.Unmarshal(raw, &items) == nil {
						pageHits.Add(1)
						local = append(local, time.Since(t0))
						continue
					}
				}
				if runCtx.Err() != nil {
					break
				}
				pageMisses.Add(1)
				ids := make([]int64, 0, len(hotPages[p]))
				for _, it := range hotPages[p] {
					ids = append(ids, it.VideoID)
				}
				cMap, err1 := repo.BatchGetFeedCards(runCtx, ids)
				sMap, err2 := repo.BatchGetFeedStats(runCtx, ids)
				if err1 != nil || err2 != nil {
					if runCtx.Err() != nil {
						break
					}
					readErrs.Add(1)
					continue
				}
				rebuilt := assembleInMem(hotPages[p], cMap, sMap)
				if b, mErr := json.Marshal(rebuilt); mErr == nil {
					_ = env.Redis.Set(runCtx, key, b, 5*time.Minute).Err()
				}
				if runCtx.Err() != nil {
					break
				}
				local = append(local, time.Since(t0))
			}
			perReaderDurs[id] = local
		}(r)
	}

	for w := range writers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)*101))
			for runCtx.Err() == nil {
				vid := hotVideoIDs[rng.Intn(len(hotVideoIDs))]
				// Write invalidates affected monolithic page caches so counters stay fresh
				for _, pIdx := range videoToPages[vid] {
					if pIdx < len(hotPages) {
						_ = env.Redis.Del(runCtx, fmt.Sprintf("bench:mono:hot:%d", pIdx)).Err()
					}
				}
				writeOps.Add(1)
				time.Sleep(2 * time.Millisecond)
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)
	after := snapshotMySQLStatus(env.SQLDB)

	all := make([]time.Duration, 0, 8192)
	for _, d := range perReaderDurs {
		all = append(all, d...)
	}
	h := pageHits.Load()
	m := pageMisses.Load()
	rate := 0.0
	if h+m > 0 {
		rate = round2(float64(h) * 100.0 / float64(h+m))
	}
	return MixedReadWriteResult{
		Mode:             "monolithic_page_invalidate_on_write",
		ReadStats:        calcLatencyStats(all, elapsed, readErrs.Load()),
		WriteOps:         writeOps.Load(),
		PageHitCount:     h,
		PageMissCount:    m,
		EffectiveHitRate: rate,
		MySQLStatusDelta: diffMySQLStatus(before, after),
	}
}

func runMixedReadWriteSep(
	ctx context.Context,
	env *BenchEnv,
	repo *infrafeed.Repository,
	feedCache *infracache.FeedCache,
	hotPages [][]*domainfeed.FeedPageItem,
	hotVideoIDs []int64,
	cards map[int64]*domainfeed.FeedCard,
	stats map[int64]*domainfeed.FeedStat,
) MixedReadWriteResult {
	_ = env.Redis.FlushDB(ctx).Err()
	for p := range hotPages {
		_ = feedCache.SetPage(ctx, fmt.Sprintf("bench:sep:hot:%d", p), &applicationfeed.FeedPage{
			Scene: domainfeed.SceneTimeline,
			Items: hotPages[p],
		}, 5*time.Minute)
	}
	_ = feedCache.SetCards(ctx, cards, 15*time.Minute)
	_ = feedCache.SetStats(ctx, stats, 15*time.Minute)

	before := snapshotMySQLStatus(env.SQLDB)
	runCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	var readErrs, writeOps, pageHits, pageMisses, detailHits, detailMisses atomic.Int64
	const readers = 50
	const writers = 10
	perReaderDurs := make([][]time.Duration, readers)
	var wg sync.WaitGroup

	start := time.Now()
	for r := range readers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)*37))
			local := make([]time.Duration, 0, 16384)
			for runCtx.Err() == nil {
				p := rng.Intn(len(hotPages))
				key := fmt.Sprintf("bench:sep:hot:%d", p)
				t0 := time.Now()
				pg, ok, err := feedCache.GetPage(runCtx, key)
				if err == nil && ok {
					pageHits.Add(1)
				} else {
					if runCtx.Err() != nil {
						break
					}
					pageMisses.Add(1)
					pg = &applicationfeed.FeedPage{Scene: domainfeed.SceneTimeline, Items: hotPages[p]}
					_ = feedCache.SetPage(runCtx, key, pg, 5*time.Minute)
				}
				ids := make([]int64, 0, len(pg.Items))
				for _, it := range pg.Items {
					ids = append(ids, it.VideoID)
				}
				cMap, err1 := feedCache.GetCards(runCtx, ids)
				sMap, err2 := feedCache.GetStats(runCtx, ids)
				if err1 != nil || err2 != nil {
					if runCtx.Err() != nil {
						break
					}
					readErrs.Add(1)
					continue
				}
				if len(cMap) == len(ids) && len(sMap) == len(ids) {
					detailHits.Add(1)
				} else {
					detailMisses.Add(1)
					if len(cMap) < len(ids) {
						loadedC, _ := repo.BatchGetFeedCards(runCtx, ids)
						_ = feedCache.SetCards(runCtx, loadedC, 15*time.Minute)
						for k, v := range loadedC {
							cMap[k] = v
						}
					}
					if len(sMap) < len(ids) {
						loadedS, _ := repo.BatchGetFeedStats(runCtx, ids)
						_ = feedCache.SetStats(runCtx, loadedS, 15*time.Minute)
						for k, v := range loadedS {
							sMap[k] = v
						}
					}
				}
				_ = assembleInMem(pg.Items, cMap, sMap)
				if runCtx.Err() != nil {
					break
				}
				local = append(local, time.Since(t0))
			}
			perReaderDurs[id] = local
		}(r)
	}

	for w := range writers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)*107))
			iter := 0
			for runCtx.Err() == nil {
				vid := hotVideoIDs[rng.Intn(len(hotVideoIDs))]
				uid := int64(1 + (id*100+iter)%500)
				active := iter%2 == 0
				iter++
				_, err := feedCache.SetActionState(runCtx, uid, vid, domaininteraction.ActionTypeLike, active, "", &domaininteraction.VideoStat{VideoID: vid, LikeCount: 100})
				if err == nil {
					writeOps.Add(1)
				}
				time.Sleep(2 * time.Millisecond)
			}
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)
	after := snapshotMySQLStatus(env.SQLDB)

	all := make([]time.Duration, 0, 32768)
	for _, d := range perReaderDurs {
		all = append(all, d...)
	}
	dh := detailHits.Load()
	dm := detailMisses.Load()
	rate := 0.0
	if dh+dm > 0 {
		rate = round2(float64(dh) * 100.0 / float64(dh+dm))
	}
	return MixedReadWriteResult{
		Mode:             "id_detail_separated_in_place_stat_update",
		ReadStats:        calcLatencyStats(all, elapsed, readErrs.Load()),
		WriteOps:         writeOps.Load(),
		PageHitCount:     pageHits.Load(),
		PageMissCount:    pageMisses.Load(),
		DetailHitCount:   dh,
		DetailMissCount:  dm,
		EffectiveHitRate: rate,
		MySQLStatusDelta: diffMySQLStatus(before, after),
	}
}

func runPersonalizedMono(
	ctx context.Context,
	env *BenchEnv,
	repo *infrafeed.Repository,
	videoIDs []int64,
	cards map[int64]*domainfeed.FeedCard,
	stats map[int64]*domainfeed.FeedStat,
) PersonalizedReuseResult {
	_ = env.Redis.FlushDB(ctx).Err()
	// Warm 10% of personalized user keys (users 1..20 out of 200); remaining 90% unique personalized pages miss monolithic cache
	for u := range 20 {
		items := pickPersonalizedIDs(u, videoIDs, cards)
		raw, _ := json.Marshal(assembleInMem(items, cards, stats))
		_ = env.Redis.Set(ctx, fmt.Sprintf("bench:mono:user:%d", u), raw, 5*time.Minute).Err()
	}

	before := snapshotMySQLStatus(env.SQLDB)
	runCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	var readErrs, pageHits, pageMisses atomic.Int64
	const readers = 50
	perReaderDurs := make([][]time.Duration, readers)
	var wg sync.WaitGroup
	start := time.Now()

	for r := range readers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)*41))
			local := make([]time.Duration, 0, 2048)
			for runCtx.Err() == nil {
				u := rng.Intn(200)
				reqID := rng.Intn(50) // each user has 50 personalized cursor/exposure variants -> low cross-user page reuse
				key := fmt.Sprintf("bench:mono:user:%d:%d", u, reqID)
				if u < 20 && reqID == 0 {
					key = fmt.Sprintf("bench:mono:user:%d", u)
				}
				t0 := time.Now()
				raw, err := env.Redis.Get(runCtx, key).Bytes()
				if err == nil {
					var items []*domainfeed.FeedItem
					if json.Unmarshal(raw, &items) == nil {
						pageHits.Add(1)
						local = append(local, time.Since(t0))
						continue
					}
				}
				if runCtx.Err() != nil {
					break
				}
				pageMisses.Add(1)
				items := pickPersonalizedIDs(u+reqID, videoIDs, cards)
				ids := make([]int64, 0, len(items))
				for _, it := range items {
					ids = append(ids, it.VideoID)
				}
				cMap, err1 := repo.BatchGetFeedCards(runCtx, ids)
				sMap, err2 := repo.BatchGetFeedStats(runCtx, ids)
				if err1 != nil || err2 != nil {
					if runCtx.Err() != nil {
						break
					}
					readErrs.Add(1)
					continue
				}
				rebuilt := assembleInMem(items, cMap, sMap)
				if b, mErr := json.Marshal(rebuilt); mErr == nil {
					_ = env.Redis.Set(runCtx, key, b, 5*time.Minute).Err()
				}
				if runCtx.Err() != nil {
					break
				}
				local = append(local, time.Since(t0))
			}
			perReaderDurs[id] = local
		}(r)
	}
	wg.Wait()
	elapsed := time.Since(start)
	after := snapshotMySQLStatus(env.SQLDB)

	all := make([]time.Duration, 0, 16384)
	for _, d := range perReaderDurs {
		all = append(all, d...)
	}
	h := pageHits.Load()
	m := pageMisses.Load()
	rate := 0.0
	if h+m > 0 {
		rate = round2(float64(h) * 100.0 / float64(h+m))
	}
	return PersonalizedReuseResult{
		Mode:             "monolithic_page_cache",
		ReadStats:        calcLatencyStats(all, elapsed, readErrs.Load()),
		PageHitRatePct:   rate,
		CardHitRatePct:   rate,
		StatHitRatePct:   rate,
		MySQLStatusDelta: diffMySQLStatus(before, after),
	}
}

func runPersonalizedSep(
	ctx context.Context,
	env *BenchEnv,
	repo *infrafeed.Repository,
	feedCache *infracache.FeedCache,
	videoIDs []int64,
	cards map[int64]*domainfeed.FeedCard,
	stats map[int64]*domainfeed.FeedStat,
) PersonalizedReuseResult {
	_ = env.Redis.FlushDB(ctx).Err()
	_ = feedCache.SetCards(ctx, cards, 15*time.Minute)
	_ = feedCache.SetStats(ctx, stats, 15*time.Minute)

	before := snapshotMySQLStatus(env.SQLDB)
	runCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()

	var readErrs, cardHits, cardReqs, statHits, statReqs atomic.Int64
	const readers = 50
	perReaderDurs := make([][]time.Duration, readers)
	var wg sync.WaitGroup
	start := time.Now()

	for r := range readers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)*43))
			local := make([]time.Duration, 0, 16384)
			for runCtx.Err() == nil {
				u := rng.Intn(200)
				reqID := rng.Intn(50)
				items := pickPersonalizedIDs(u+reqID, videoIDs, cards)
				ids := make([]int64, 0, len(items))
				for _, it := range items {
					ids = append(ids, it.VideoID)
				}
				t0 := time.Now()
				cMap, err1 := feedCache.GetCards(runCtx, ids)
				sMap, err2 := feedCache.GetStats(runCtx, ids)
				if err1 != nil || err2 != nil {
					if runCtx.Err() != nil {
						break
					}
					readErrs.Add(1)
					continue
				}
				cardReqs.Add(int64(len(ids)))
				cardHits.Add(int64(len(cMap)))
				statReqs.Add(int64(len(ids)))
				statHits.Add(int64(len(sMap)))

				if len(cMap) < len(ids) {
					loadedC, _ := repo.BatchGetFeedCards(runCtx, ids)
					_ = feedCache.SetCards(runCtx, loadedC, 15*time.Minute)
					for k, v := range loadedC {
						cMap[k] = v
					}
				}
				if len(sMap) < len(ids) {
					loadedS, _ := repo.BatchGetFeedStats(runCtx, ids)
					_ = feedCache.SetStats(runCtx, loadedS, 15*time.Minute)
					for k, v := range loadedS {
						sMap[k] = v
					}
				}
				_ = assembleInMem(items, cMap, sMap)
				if runCtx.Err() != nil {
					break
				}
				local = append(local, time.Since(t0))
			}
			perReaderDurs[id] = local
		}(r)
	}
	wg.Wait()
	elapsed := time.Since(start)
	after := snapshotMySQLStatus(env.SQLDB)

	all := make([]time.Duration, 0, 32768)
	for _, d := range perReaderDurs {
		all = append(all, d...)
	}
	cRate := 0.0
	if cardReqs.Load() > 0 {
		cRate = round2(float64(cardHits.Load()) * 100.0 / float64(cardReqs.Load()))
	}
	sRate := 0.0
	if statReqs.Load() > 0 {
		sRate = round2(float64(statHits.Load()) * 100.0 / float64(statReqs.Load()))
	}
	return PersonalizedReuseResult{
		Mode:             "id_detail_separated_cache",
		ReadStats:        calcLatencyStats(all, elapsed, readErrs.Load()),
		PageHitRatePct:   0,
		CardHitRatePct:   cRate,
		StatHitRatePct:   sRate,
		MySQLStatusDelta: diffMySQLStatus(before, after),
	}
}

func pickPersonalizedIDs(seed int, videoIDs []int64, cards map[int64]*domainfeed.FeedCard) []*domainfeed.FeedPageItem {
	out := make([]*domainfeed.FeedPageItem, 0, 10)
	n := len(videoIDs)
	for i := range 10 {
		idx := (seed*17 + i*29) % n
		vid := videoIDs[idx]
		c := cards[vid]
		out = append(out, &domainfeed.FeedPageItem{
			VideoID:     vid,
			AuthorID:    c.AuthorID,
			PublishedAt: c.PublishedAt,
		})
	}
	return out
}
