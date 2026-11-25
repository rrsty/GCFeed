package applicationinteraction

import (
	domaininteraction "GCFeed/internal/domain/interaction"
	inframetrics "GCFeed/internal/infra/metrics"
	"context"
	"errors"
	"log"
	"sync"
	"time"
)

const (
	defaultBatchFlushInterval = 200 * time.Millisecond
	defaultBatchMaxEvents     = 50
)

type ActionEventConsumer interface {
	ConsumeActionChanged(ctx context.Context, handler func(context.Context, *ActionChangedEvent) error) error
}

// ActionRecordRepository 支持把明细落库与统计攒批刷库解耦，避免热点视频单行并发写放大。
type ActionRecordRepository interface {
	SetActionRecordOnly(ctx context.Context, userID int64, videoID int64, actionType string, active bool, idempotencyKey string, occurredAt ...time.Time) (*domaininteraction.Action, int, error)
	BatchAddVideoStatDeltas(ctx context.Context, deltas []domaininteraction.VideoStatDelta) error
}

type permanentError struct {
	err error
}

func (e permanentError) Error() string {
	return e.err.Error()
}

func (e permanentError) Unwrap() error {
	return e.err
}

// Permanent 标识不可重试的业务错误，供 MQ 消费者直接转入死信队列避免毒丸死循环。
func (e permanentError) Permanent() bool {
	return true
}

type ActionWorkerOption func(*ActionWorker)

type ActionWorker struct {
	repo           domaininteraction.Repository
	consumer       ActionEventConsumer
	batchRepo      ActionRecordRepository
	flushInterval  time.Duration
	maxBatchEvents int

	mu            sync.Mutex
	pending       map[int64]*domaininteraction.VideoStatDelta
	pendingEvents int
	flushDone     chan struct{}
}

func NewActionWorker(repo domaininteraction.Repository, consumer ActionEventConsumer, options ...ActionWorkerOption) *ActionWorker {
	worker := &ActionWorker{
		repo:     repo,
		consumer: consumer,
		pending:  map[int64]*domaininteraction.VideoStatDelta{},
	}
	for _, option := range options {
		option(worker)
	}
	return worker
}

// WithBatchStatFlush 启用窗口攒批刷库（路线 A）：明细逐条幂等落库，video_stat 增量按 video_id 合并批量原子更新。
func WithBatchStatFlush(flushInterval time.Duration, maxBatchEvents int) ActionWorkerOption {
	return func(w *ActionWorker) {
		batchRepo, ok := w.repo.(ActionRecordRepository)
		if !ok {
			return
		}
		if flushInterval <= 0 {
			flushInterval = defaultBatchFlushInterval
		}
		if maxBatchEvents <= 0 {
			maxBatchEvents = defaultBatchMaxEvents
		}
		w.batchRepo = batchRepo
		w.flushInterval = flushInterval
		w.maxBatchEvents = maxBatchEvents
	}
}

func (w *ActionWorker) Start(ctx context.Context) error {
	if w == nil || w.consumer == nil {
		return nil
	}
	if w.batchRepo != nil {
		w.startFlushLoop(ctx)
	}
	return w.consumer.ConsumeActionChanged(ctx, w.HandleActionChanged)
}

func (w *ActionWorker) startFlushLoop(ctx context.Context) {
	done := make(chan struct{})
	w.mu.Lock()
	w.flushDone = done
	w.mu.Unlock()

	go func() {
		defer close(done)
		ticker := time.NewTicker(w.flushInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				if err := w.Flush(flushCtx); err != nil {
					log.Printf("action worker final batch flush failed: %v", err)
				}
				cancel()
				return
			case <-ticker.C:
				flushCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
				if err := w.Flush(flushCtx); err != nil {
					log.Printf("action worker periodic batch flush failed: %v", err)
				}
				cancel()
			}
		}
	}()
}

// Close 等待后台攒批循环退出并把残留增量刷入数据库。
func (w *ActionWorker) Close(ctx context.Context) error {
	if w == nil || w.batchRepo == nil {
		return nil
	}
	w.mu.Lock()
	done := w.flushDone
	w.mu.Unlock()
	if done != nil {
		select {
		case <-done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return w.Flush(ctx)
}

func (w *ActionWorker) HandleActionChanged(ctx context.Context, event *ActionChangedEvent) error {
	start := time.Now()
	var err error
	defer func() {
		inframetrics.ObserveWorkerJob("interaction_action_changed", time.Since(start), err)
	}()

	if event == nil {
		return nil
	}
	if w.batchRepo != nil {
		var delta int
		_, delta, err = w.batchRepo.SetActionRecordOnly(ctx, event.UserID, event.VideoID, event.ActionType, event.Active, event.IdempotencyKey, event.OccurredAt)
		if err != nil {
			err = classifyActionWorkerError(err)
			return err
		}
		if delta != 0 {
			if shouldFlush := w.bufferStatDelta(event.VideoID, event.ActionType, delta); shouldFlush {
				if flushErr := w.Flush(ctx); flushErr != nil {
					log.Printf("action worker threshold batch flush failed (will retry): %v", flushErr)
				}
			}
		}
		return nil
	}

	_, _, _, err = w.repo.SetAction(ctx, event.UserID, event.VideoID, event.ActionType, event.Active, event.IdempotencyKey, event.OccurredAt)
	if err != nil {
		err = classifyActionWorkerError(err)
	}
	return err
}

func (w *ActionWorker) bufferStatDelta(videoID int64, actionType string, delta int) bool {
	normalized, err := domaininteraction.NormalizeActionType(actionType)
	if err != nil || videoID <= 0 || delta == 0 {
		return false
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	entry := w.pending[videoID]
	if entry == nil {
		entry = &domaininteraction.VideoStatDelta{VideoID: videoID}
		w.pending[videoID] = entry
	}
	if normalized == domaininteraction.ActionTypeLike {
		entry.LikeDelta += delta
	} else {
		entry.FavoriteDelta += delta
	}
	w.pendingEvents++
	return w.maxBatchEvents > 0 && w.pendingEvents >= w.maxBatchEvents
}

// Flush 将当前窗口内聚合的视频增量一次性批量刷入 MySQL；失败时回放缓冲避免计数丢失。
func (w *ActionWorker) Flush(ctx context.Context) error {
	if w == nil || w.batchRepo == nil {
		return nil
	}

	w.mu.Lock()
	if len(w.pending) == 0 {
		w.pendingEvents = 0
		w.mu.Unlock()
		return nil
	}
	batch := make([]domaininteraction.VideoStatDelta, 0, len(w.pending))
	for _, item := range w.pending {
		if item.LikeDelta == 0 && item.FavoriteDelta == 0 {
			continue
		}
		batch = append(batch, *item)
	}
	w.pending = map[int64]*domaininteraction.VideoStatDelta{}
	w.pendingEvents = 0
	w.mu.Unlock()

	if len(batch) == 0 {
		return nil
	}

	start := time.Now()
	err := w.batchRepo.BatchAddVideoStatDeltas(ctx, batch)
	inframetrics.ObserveWorkerJob("interaction_stat_batch_flush", time.Since(start), err)
	if err != nil {
		// 刷库失败时放回缓冲，等待下一轮周期重试，防止计数增量丢失。
		w.mu.Lock()
		for _, item := range batch {
			entry := w.pending[item.VideoID]
			if entry == nil {
				copied := item
				w.pending[item.VideoID] = &copied
			} else {
				entry.LikeDelta += item.LikeDelta
				entry.FavoriteDelta += item.FavoriteDelta
			}
			w.pendingEvents++
		}
		w.mu.Unlock()
		return err
	}
	return nil
}

func classifyActionWorkerError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, domaininteraction.ErrVideoNotFound) ||
		errors.Is(err, domaininteraction.ErrInvalidActionType) ||
		errors.Is(err, domaininteraction.ErrInvalidUserID) ||
		errors.Is(err, domaininteraction.ErrInvalidVideoID) {
		return permanentError{err: err}
	}
	return err
}
