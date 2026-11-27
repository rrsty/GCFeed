package main

import (
	applicationinteraction "GCFeed/internal/application/interaction"
	domaininteraction "GCFeed/internal/domain/interaction"
	infracache "GCFeed/internal/infra/cache"
	infrainteraction "GCFeed/internal/infra/persistence/interaction"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const benchVideoID int64 = 49990

type RedisWriteGroupResult struct {
	Scenario          string       `json:"scenario"`
	Mode              string       `json:"mode"`
	Concurrency       int          `json:"concurrency"`
	Stats             LatencyStats `json:"stats"`
	ConflictErrors    int64        `json:"conflict_errors"`
	ConflictRatePct   float64      `json:"conflict_rate_pct"`
	RedisCommandsDelta int64       `json:"redis_commands_delta"`
	CommandsPerOp     float64      `json:"commands_per_op"`
	RedisKeyCount     int64        `json:"redis_key_count"`
}

type WorkerFlushGroupResult struct {
	Mode                 string           `json:"mode"`
	TotalEvents          int              `json:"total_events"`
	Concurrency          int              `json:"concurrency"`
	Errors               int64            `json:"errors"`
	ElapsedSec           float64          `json:"elapsed_sec"`
	EventsPerSec         float64          `json:"events_per_sec"`
	SetActionCalls       int64            `json:"set_action_calls"`
	StatFlushCalls       int64            `json:"stat_flush_calls"`
	DBVideoStatLikeCount int              `json:"db_video_stat_like_count"`
	DBActiveActionRows   int              `json:"db_active_action_rows"`
	Consistent           bool             `json:"consistent"`
	MySQLStatusDelta     map[string]int64 `json:"mysql_status_delta"`
}

type WriteMicroSuiteResult struct {
	Timestamp     string                   `json:"timestamp"`
	BenchVideoID  int64                    `json:"bench_video_id"`
	RedisWrite    []RedisWriteGroupResult  `json:"redis_write"`
	WorkerDBFlush []WorkerFlushGroupResult `json:"worker_db_flush"`
}

type countingActionRepo struct {
	*infrainteraction.Repository
	recordCalls atomic.Int64
	flushCalls  atomic.Int64
}

func (r *countingActionRepo) SetActionRecordOnly(ctx context.Context, userID int64, videoID int64, actionType string, active bool, idempotencyKey string, occurredAt ...time.Time) (*domaininteraction.Action, int, error) {
	r.recordCalls.Add(1)
	return r.Repository.SetActionRecordOnly(ctx, userID, videoID, actionType, active, idempotencyKey, occurredAt...)
}

func (r *countingActionRepo) BatchAddVideoStatDeltas(ctx context.Context, deltas []domaininteraction.VideoStatDelta) error {
	r.flushCalls.Add(1)
	return r.Repository.BatchAddVideoStatDeltas(ctx, deltas)
}

func (r *countingActionRepo) SetAction(ctx context.Context, userID int64, videoID int64, actionType string, active bool, idempotencyKey string, occurredAt ...time.Time) (*domaininteraction.Action, int, int, error) {
	r.recordCalls.Add(1)
	r.flushCalls.Add(1)
	return r.Repository.SetAction(ctx, userID, videoID, actionType, active, idempotencyKey, occurredAt...)
}

func runWriteMicroSuite(env *BenchEnv) error {
	ctx := context.Background()
	fmt.Println("=== Running Suite 3: High-Concurrency Write Microbenchmarks (Redis WATCH vs Lua & Worker Batch Flush) ===")

	legacyStore := NewLegacyWatchFeedCache(env.Redis)
	luaStore := infracache.NewFeedCache(env.Redis)

	var redisResults []RedisWriteGroupResult

	// 3A-1: Multi-user contention on hot video 49990
	for _, conc := range []int{50, 100, 200} {
		for _, mode := range []string{"legacy_watch_16shard", "lua_single_hash"} {
			r := benchRedisWrite(ctx, env, mode, "multi_user_hot_video", conc, 10*time.Second, false, legacyStore, luaStore)
			redisResults = append(redisResults, r)
			fmt.Printf("  3A MultiUser conc=%3d mode=%-21s | QPS=%8.2f p95=%.3fms cmds/op=%.2f keys=%d err=%d\n",
				conc, mode, r.Stats.QPS, r.Stats.P95MS, r.CommandsPerOp, r.RedisKeyCount, r.ConflictErrors)
		}
	}

	// 3A-2: Single-user high-concurrency burst contention (exposes WATCH optimistic lock failure rate)
	for _, mode := range []string{"legacy_watch_16shard", "lua_single_hash"} {
		r := benchRedisWrite(ctx, env, mode, "single_user_same_key_burst", 50, 5*time.Second, true, legacyStore, luaStore)
		redisResults = append(redisResults, r)
		fmt.Printf("  3A SameUser  conc=%3d mode=%-21s | QPS=%8.2f p95=%.3fms conflict=%d (%.2f%%) cmds/op=%.2f\n",
			50, mode, r.Stats.QPS, r.Stats.P95MS, r.ConflictErrors, r.ConflictRatePct, r.CommandsPerOp)
	}

	// Backup original video_stat for benchVideoID and restore on exit
	origLike, origComment, origFav, err := backupVideoStat(ctx, env, benchVideoID)
	if err != nil {
		return fmt.Errorf("backup video_stat failed: %w", err)
	}
	defer func() {
		_ = restoreVideoStatAndCleanup(context.Background(), env, benchVideoID, origLike, origComment, origFav)
	}()

	// 3B: Worker DB flush (1500 events: 1000 distinct users like + 500 users unlike targeting video 49990)
	baseRepo := infrainteraction.New(env.GormDB)
	phases := [][]*applicationinteraction.ActionChangedEvent{
		buildBenchActionPhase(1000, true, benchVideoID, 0),
		buildBenchActionPhase(500, false, benchVideoID, 1000),
	}

	var workerResults []WorkerFlushGroupResult
	for _, batchMode := range []bool{false, true} {
		if err := resetBenchVideoWithCanceledRows(ctx, env, benchVideoID, 1000); err != nil {
			return err
		}
		res, err := runWorkerFlushBench(ctx, env, baseRepo, phases, 20, batchMode)
		if err != nil {
			return err
		}
		workerResults = append(workerResults, res)
		fmt.Printf("  3B Worker(%s): events=%d err=%d elapsed=%.3fs throughput=%.1f ev/s statFlushCalls=%d Com_update=%d lockWaits=%d like_count=%d/%d ok=%v\n",
			res.Mode, res.TotalEvents, res.Errors, res.ElapsedSec, res.EventsPerSec, res.StatFlushCalls,
			res.MySQLStatusDelta["Com_update"], res.MySQLStatusDelta["Innodb_row_lock_waits"],
			res.DBVideoStatLikeCount, res.DBActiveActionRows, res.Consistent)
	}

	return writeJSON(env.OutDir, "suite3_write_microbench.json", WriteMicroSuiteResult{
		Timestamp:     time.Now().Format(time.RFC3339),
		BenchVideoID:  benchVideoID,
		RedisWrite:    redisResults,
		WorkerDBFlush: workerResults,
	})
}

func benchRedisWrite(
	ctx context.Context,
	env *BenchEnv,
	mode string,
	scenario string,
	concurrency int,
	duration time.Duration,
	sameUser bool,
	legacyStore *LegacyWatchFeedCache,
	luaStore *infracache.FeedCache,
) RedisWriteGroupResult {
	_ = env.Redis.FlushDB(ctx).Err()
	cmdBefore := snapshotRedisCommands(ctx, env.Redis)

	runCtx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()

	var errCnt atomic.Int64
	perWorker := make([][]time.Duration, concurrency)
	var wg sync.WaitGroup
	initStat := &domaininteraction.VideoStat{VideoID: benchVideoID, LikeCount: 100}

	start := time.Now()
	for w := range concurrency {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			local := make([]time.Duration, 0, 4096)
			uid := int64(1 + (workerID % 200))
			if sameUser {
				uid = 1
			}
			iter := 0
			for runCtx.Err() == nil {
				active := iter%2 == 0
				idem := fmt.Sprintf("bench-%d-%d", workerID, iter)
				iter++
				t0 := time.Now()
				var sErr error
				if mode == "legacy_watch_16shard" {
					_, sErr = legacyStore.SetActionState(runCtx, uid, benchVideoID, domaininteraction.ActionTypeLike, active, idem, initStat)
				} else {
					_, sErr = luaStore.SetActionState(runCtx, uid, benchVideoID, domaininteraction.ActionTypeLike, active, idem, initStat)
				}
				d := time.Since(t0)
				if runCtx.Err() != nil {
					break
				}
				if sErr != nil {
					errCnt.Add(1)
				} else {
					local = append(local, d)
				}
			}
			perWorker[workerID] = local
		}(w)
	}
	wg.Wait()
	elapsed := time.Since(start)

	cmdAfter := snapshotRedisCommands(ctx, env.Redis)
	keyCount, _ := env.Redis.DBSize(ctx).Result()
	all := make([]time.Duration, 0, 32768)
	for _, d := range perWorker {
		all = append(all, d...)
	}
	stats := calcLatencyStats(all, elapsed, errCnt.Load())
	totalAttempts := stats.Count + stats.Errors
	cmdsDelta := cmdAfter - cmdBefore
	cmdsPerOp := 0.0
	if totalAttempts > 0 {
		cmdsPerOp = round2(float64(cmdsDelta) / float64(totalAttempts))
	}
	conflictRate := 0.0
	if totalAttempts > 0 {
		conflictRate = round2(float64(stats.Errors) * 100.0 / float64(totalAttempts))
	}
	return RedisWriteGroupResult{
		Scenario:           scenario,
		Mode:               mode,
		Concurrency:        concurrency,
		Stats:              stats,
		ConflictErrors:     stats.Errors,
		ConflictRatePct:    conflictRate,
		RedisCommandsDelta: cmdsDelta,
		CommandsPerOp:      cmdsPerOp,
		RedisKeyCount:      keyCount,
	}
}

func buildBenchActionPhase(userCount int, active bool, videoID int64, seqOffset int) []*applicationinteraction.ActionChangedEvent {
	events := make([]*applicationinteraction.ActionChangedEvent, 0, userCount)
	baseTime := time.Now().UTC()
	for u := 1; u <= userCount; u++ {
		seq := seqOffset + u
		events = append(events, &applicationinteraction.ActionChangedEvent{
			EventID:        fmt.Sprintf("ev-%d", seq),
			UserID:         int64(u),
			VideoID:        videoID,
			ActionType:     domaininteraction.ActionTypeLike,
			Active:         active,
			IdempotencyKey: fmt.Sprintf("bench-w-%d-%d", u, seq),
			OccurredAt:     baseTime.Add(time.Duration(seq) * time.Microsecond),
		})
	}
	return events
}

func runWorkerFlushBench(
	ctx context.Context,
	env *BenchEnv,
	baseRepo *infrainteraction.Repository,
	phases [][]*applicationinteraction.ActionChangedEvent,
	concurrency int,
	batchFlush bool,
) (WorkerFlushGroupResult, error) {
	cRepo := &countingActionRepo{Repository: baseRepo}
	var opts []applicationinteraction.ActionWorkerOption
	mode := "per_event_sync_stat_update"
	if batchFlush {
		opts = append(opts, applicationinteraction.WithBatchStatFlush(200*time.Millisecond, 50))
		mode = "batch_stat_flush_200ms_50"
	}
	worker := applicationinteraction.NewActionWorker(cRepo, nil, opts...)

	workerCtx, cancelWorker := context.WithCancel(ctx)
	defer cancelWorker()
	if batchFlush {
		if err := worker.Start(workerCtx); err != nil {
			return WorkerFlushGroupResult{}, err
		}
	}

	totalEvents := 0
	for _, p := range phases {
		totalEvents += len(p)
	}

	before := snapshotMySQLStatus(env.SQLDB)
	start := time.Now()
	var errCnt atomic.Int64

	for _, events := range phases {
		ch := make(chan *applicationinteraction.ActionChangedEvent, len(events))
		for _, ev := range events {
			ch <- ev
		}
		close(ch)

		var wg sync.WaitGroup
		for range concurrency {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for ev := range ch {
					if err := worker.HandleActionChanged(ctx, ev); err != nil {
						errCnt.Add(1)
					}
				}
			}()
		}
		wg.Wait()
	}

	if batchFlush {
		cancelWorker()
		flushCtx, cancelFlush := context.WithTimeout(context.Background(), 10*time.Second)
		_ = worker.Close(flushCtx)
		cancelFlush()
	}
	elapsed := time.Since(start)
	after := snapshotMySQLStatus(env.SQLDB)

	var statLike int
	var activeRows int
	_ = env.SQLDB.QueryRowContext(ctx, "SELECT like_count FROM video_stat WHERE video_id = ?", benchVideoID).Scan(&statLike)
	_ = env.SQLDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM interaction_action WHERE video_id = ? AND action_type = 'LIKE' AND status = 1", benchVideoID).Scan(&activeRows)

	return WorkerFlushGroupResult{
		Mode:                 mode,
		TotalEvents:          totalEvents,
		Concurrency:          concurrency,
		Errors:               errCnt.Load(),
		ElapsedSec:           round4(elapsed.Seconds()),
		EventsPerSec:         round2(float64(totalEvents) / elapsed.Seconds()),
		SetActionCalls:       cRepo.recordCalls.Load(),
		StatFlushCalls:       cRepo.flushCalls.Load(),
		DBVideoStatLikeCount: statLike,
		DBActiveActionRows:   activeRows,
		Consistent:           statLike == activeRows && errCnt.Load() == 0,
		MySQLStatusDelta:     diffMySQLStatus(before, after),
	}, nil
}

func backupVideoStat(ctx context.Context, env *BenchEnv, videoID int64) (int, int, int, error) {
	var l, c, f int
	err := env.SQLDB.QueryRowContext(ctx, "SELECT like_count, comment_count, favorite_count FROM video_stat WHERE video_id = ?", videoID).Scan(&l, &c, &f)
	return l, c, f, err
}

func resetBenchVideoWithCanceledRows(ctx context.Context, env *BenchEnv, videoID int64, userCount int) error {
	if err := resetBenchVideoForWrite(ctx, env, videoID); err != nil {
		return err
	}
	tx, err := env.SQLDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	stmt, err := tx.PrepareContext(ctx, "INSERT INTO interaction_action (user_id, video_id, action_type, status, last_event_at_ns, created_at, updated_at) VALUES (?, ?, 'LIKE', 2, 0, NOW(3), NOW(3))")
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	for u := 1; u <= userCount; u++ {
		if _, err := stmt.ExecContext(ctx, u, videoID); err != nil {
			_ = stmt.Close()
			_ = tx.Rollback()
			return err
		}
	}
	_ = stmt.Close()
	return tx.Commit()
}

func resetBenchVideoForWrite(ctx context.Context, env *BenchEnv, videoID int64) error {
	if _, err := env.SQLDB.ExecContext(ctx, "DELETE FROM interaction_action WHERE video_id = ?", videoID); err != nil {
		return err
	}
	if _, err := env.SQLDB.ExecContext(ctx, "UPDATE video_stat SET like_count = 0 WHERE video_id = ?", videoID); err != nil {
		return err
	}
	return nil
}

func restoreVideoStatAndCleanup(ctx context.Context, env *BenchEnv, videoID int64, like, comment, fav int) error {
	_, _ = env.SQLDB.ExecContext(ctx, "DELETE FROM interaction_action WHERE video_id = ?", videoID)
	_, _ = env.SQLDB.ExecContext(ctx, "UPDATE video_stat SET like_count = ?, comment_count = ?, favorite_count = ? WHERE video_id = ?", like, comment, fav, videoID)
	return env.Redis.FlushDB(ctx).Err()
}
