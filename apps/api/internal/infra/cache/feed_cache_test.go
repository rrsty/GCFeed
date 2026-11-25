package infracache

import (
	domainfeed "GCFeed/internal/domain/feed"
	domaininteraction "GCFeed/internal/domain/interaction"
	"context"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

type fakeRedisClient struct {
	redis.Cmdable
	mu       sync.Mutex
	hashes   map[string]map[string]string
	zsets    map[string]map[string]float64
	zremLog  map[string][]string
	evalLog  []fakeEvalCall
	evalFunc func(script string, keys []string, args ...any) (any, error)
}

type fakeEvalCall struct {
	script string
	keys   []string
	args   []any
}

func newFakeRedisClient() *fakeRedisClient {
	return &fakeRedisClient{
		hashes:  map[string]map[string]string{},
		zsets:   map[string]map[string]float64{},
		zremLog: map[string][]string{},
	}
}

func (r *fakeRedisClient) Pipeline() redis.Pipeliner {
	return &fakePipeliner{client: r}
}

func (r *fakeRedisClient) Eval(ctx context.Context, script string, keys []string, args ...any) *redis.Cmd {
	return r.runScript(script, keys, args...)
}

func (r *fakeRedisClient) EvalSha(ctx context.Context, sha1 string, keys []string, args ...any) *redis.Cmd {
	return r.runScript(sha1, keys, args...)
}

func (r *fakeRedisClient) EvalRO(ctx context.Context, script string, keys []string, args ...any) *redis.Cmd {
	return r.runScript(script, keys, args...)
}

func (r *fakeRedisClient) EvalShaRO(ctx context.Context, sha1 string, keys []string, args ...any) *redis.Cmd {
	return r.runScript(sha1, keys, args...)
}

func (r *fakeRedisClient) ScriptExists(ctx context.Context, hashes ...string) *redis.BoolSliceCmd {
	res := make([]bool, len(hashes))
	return redis.NewBoolSliceResult(res, nil)
}

func (r *fakeRedisClient) ScriptLoad(ctx context.Context, script string) *redis.StringCmd {
	return redis.NewStringResult("", nil)
}

func (r *fakeRedisClient) runScript(script string, keys []string, args ...any) *redis.Cmd {
	r.mu.Lock()
	r.evalLog = append(r.evalLog, fakeEvalCall{script: script, keys: keys, args: args})
	fn := r.evalFunc
	r.mu.Unlock()
	if fn != nil {
		val, err := fn(script, keys, args...)
		return redis.NewCmdResult(val, err)
	}
	return r.simulateLuaScript(keys, args...)
}

// simulateLuaScript 精确模拟 setActionStateScript 与 syncCommentStatScript 在 Redis 中的原子行为。
func (r *fakeRedisClient) simulateLuaScript(keys []string, args ...any) *redis.Cmd {
	r.mu.Lock()
	defer r.mu.Unlock()

	if len(keys) == 1 && len(args) == 4 {
		// syncCommentStatScript
		statKey := keys[0]
		likeCount := anyToInt(args[0])
		commentCount := anyToInt(args[1])
		favCount := anyToInt(args[2])
		if existing, ok := r.hashes[statKey]; ok {
			existing["comment_count"] = strconv.Itoa(commentCount)
		} else {
			r.hashes[statKey] = map[string]string{
				"like_count":     strconv.Itoa(likeCount),
				"comment_count":  strconv.Itoa(commentCount),
				"favorite_count": strconv.Itoa(favCount),
			}
		}
		return redis.NewCmdResult(int64(1), nil)
	}

	// setActionStateScript
	actionKey := keys[0]
	statKey := keys[1]
	targetStatus := anyToInt(args[0])
	idemKey := anyToString(args[1])
	updatedAt := anyToString(args[2])
	field := anyToString(args[5])
	initLike := anyToInt(args[6])
	initComment := anyToInt(args[7])
	initFav := anyToInt(args[8])
	hasInitAction := anyToInt(args[9])
	initActionActive := anyToInt(args[10])

	storedStatus := 0
	storedIdem := ""
	if actionHash, ok := r.hashes[actionKey]; ok {
		storedStatus, _ = strconv.Atoi(actionHash["status"])
		storedIdem = actionHash["idempotency_key"]
	}

	effectiveStatus := targetStatus
	delta := 0
	if idemKey != "" && storedIdem == idemKey && storedStatus != 0 {
		effectiveStatus = storedStatus
		delta = 0
	} else {
		if storedStatus == 0 {
			if hasInitAction == 1 {
				if initActionActive == 1 && targetStatus != 1 {
					delta = -1
				} else if initActionActive == 0 && targetStatus == 1 {
					delta = 1
				}
			} else if targetStatus == 1 {
				delta = 1
			}
		} else if storedStatus != targetStatus {
			if targetStatus == 1 {
				delta = 1
			} else {
				delta = -1
			}
		}
	}

	r.hashes[actionKey] = map[string]string{
		"status":          strconv.Itoa(effectiveStatus),
		"idempotency_key": idemKey,
		"updated_at":      updatedAt,
	}

	statHash, exists := r.hashes[statKey]
	if !exists {
		statHash = map[string]string{
			"like_count":     strconv.Itoa(initLike),
			"comment_count":  strconv.Itoa(initComment),
			"favorite_count": strconv.Itoa(initFav),
		}
		r.hashes[statKey] = statHash
	}
	if delta != 0 {
		cur, _ := strconv.Atoi(statHash[field])
		next := clampRedisCount(cur + delta)
		statHash[field] = strconv.Itoa(next)
	}

	likeVal, _ := strconv.Atoi(statHash["like_count"])
	commentVal, _ := strconv.Atoi(statHash["comment_count"])
	favVal, _ := strconv.Atoi(statHash["favorite_count"])
	return redis.NewCmdResult([]any{int64(effectiveStatus), int64(delta), int64(likeVal), int64(commentVal), int64(favVal)}, nil)
}

func (r *fakeRedisClient) Exists(ctx context.Context, keys ...string) *redis.IntCmd {
	r.mu.Lock()
	defer r.mu.Unlock()
	var count int64
	for _, key := range keys {
		if _, ok := r.hashes[key]; ok {
			count++
		} else if _, ok := r.zsets[key]; ok {
			count++
		}
	}
	return redis.NewIntResult(count, nil)
}

func (r *fakeRedisClient) ZRem(ctx context.Context, key string, members ...any) *redis.IntCmd {
	r.mu.Lock()
	defer r.mu.Unlock()
	var removed int64
	zset := r.zsets[key]
	for _, member := range members {
		m := anyToString(member)
		r.zremLog[key] = append(r.zremLog[key], m)
		if zset != nil {
			if _, ok := zset[m]; ok {
				delete(zset, m)
				removed++
			}
		}
	}
	return redis.NewIntResult(removed, nil)
}

type fakePipeliner struct {
	redis.Pipeliner
	client *fakeRedisClient
	ops    []func()
}

func (p *fakePipeliner) HGetAll(ctx context.Context, key string) *redis.MapStringStringCmd {
	cmd := redis.NewMapStringStringCmd(ctx)
	p.ops = append(p.ops, func() {
		p.client.mu.Lock()
		src := p.client.hashes[key]
		copied := make(map[string]string, len(src))
		for k, v := range src {
			copied[k] = v
		}
		p.client.mu.Unlock()
		cmd.SetVal(copied)
	})
	return cmd
}

func (p *fakePipeliner) HGet(ctx context.Context, key, field string) *redis.StringCmd {
	cmd := redis.NewStringCmd(ctx)
	p.ops = append(p.ops, func() {
		p.client.mu.Lock()
		val, ok := p.client.hashes[key][field]
		p.client.mu.Unlock()
		if !ok {
			cmd.SetErr(redis.Nil)
			return
		}
		cmd.SetVal(val)
	})
	return cmd
}

func (p *fakePipeliner) HSetNX(ctx context.Context, key, field string, value any) *redis.BoolCmd {
	cmd := redis.NewBoolCmd(ctx)
	p.ops = append(p.ops, func() {
		p.client.mu.Lock()
		h := p.client.hashes[key]
		if h == nil {
			h = map[string]string{}
			p.client.hashes[key] = h
		}
		if _, exists := h[field]; !exists {
			h[field] = anyToString(value)
			cmd.SetVal(true)
		} else {
			cmd.SetVal(false)
		}
		p.client.mu.Unlock()
	})
	return cmd
}

func (p *fakePipeliner) ZAdd(ctx context.Context, key string, members ...redis.Z) *redis.IntCmd {
	cmd := redis.NewIntCmd(ctx)
	p.ops = append(p.ops, func() {
		p.client.mu.Lock()
		zset := p.client.zsets[key]
		if zset == nil {
			zset = map[string]float64{}
			p.client.zsets[key] = zset
		}
		var added int64
		for _, m := range members {
			member := anyToString(m.Member)
			if _, exists := zset[member]; !exists {
				added++
			}
			zset[member] = m.Score
		}
		p.client.mu.Unlock()
		cmd.SetVal(added)
	})
	return cmd
}

func (p *fakePipeliner) ZRemRangeByRank(ctx context.Context, key string, start, stop int64) *redis.IntCmd {
	cmd := redis.NewIntCmd(ctx)
	p.ops = append(p.ops, func() { cmd.SetVal(0) })
	return cmd
}

func (p *fakePipeliner) ZCard(ctx context.Context, key string) *redis.IntCmd {
	cmd := redis.NewIntCmd(ctx)
	p.ops = append(p.ops, func() {
		p.client.mu.Lock()
		n := int64(len(p.client.zsets[key]))
		p.client.mu.Unlock()
		cmd.SetVal(n)
	})
	return cmd
}

func (p *fakePipeliner) ZRevRangeByScore(ctx context.Context, key string, opt *redis.ZRangeBy) *redis.StringSliceCmd {
	cmd := redis.NewStringSliceCmd(ctx)
	p.ops = append(p.ops, func() {
		p.client.mu.Lock()
		zset := p.client.zsets[key]
		type entry struct {
			member string
			score  float64
		}
		entries := make([]entry, 0, len(zset))
		for m, s := range zset {
			if opt != nil && opt.Max != "" && opt.Max != "+inf" {
				exclusive := strings.HasPrefix(opt.Max, "(")
				rawMax := strings.TrimPrefix(opt.Max, "(")
				maxVal, err := strconv.ParseFloat(rawMax, 64)
				if err == nil {
					if exclusive && s >= maxVal {
						continue
					}
					if !exclusive && s > maxVal {
						continue
					}
				}
			}
			entries = append(entries, entry{member: m, score: s})
		}
		p.client.mu.Unlock()

		sort.Slice(entries, func(i, j int) bool {
			if entries[i].score == entries[j].score {
				return entries[i].member > entries[j].member
			}
			return entries[i].score > entries[j].score
		})
		if opt != nil && opt.Count > 0 && int64(len(entries)) > opt.Count {
			entries = entries[:opt.Count]
		}
		res := make([]string, 0, len(entries))
		for _, e := range entries {
			res = append(res, e.member)
		}
		cmd.SetVal(res)
	})
	return cmd
}

func (p *fakePipeliner) Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd {
	cmd := redis.NewBoolCmd(ctx)
	p.ops = append(p.ops, func() { cmd.SetVal(true) })
	return cmd
}

func (p *fakePipeliner) Exec(ctx context.Context) ([]redis.Cmder, error) {
	for _, op := range p.ops {
		op()
	}
	return nil, nil
}

func anyToInt(v any) int {
	switch t := v.(type) {
	case int:
		return t
	case int64:
		return int(t)
	case string:
		n, _ := strconv.Atoi(t)
		return n
	default:
		return 0
	}
}

func anyToString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case int:
		return strconv.Itoa(t)
	case int64:
		return strconv.FormatInt(t, 10)
	default:
		return ""
	}
}

func TestRouteALuaActionStateAndSingleHashCommentSync(t *testing.T) {
	ctx := context.Background()
	redisClient := newFakeRedisClient()
	cache := NewFeedCache(redisClient)
	videoID := int64(1001)

	// 1. 用户点赞：Lua 脚本初始化单 Hash 并累加 like_count。
	res, err := cache.SetActionState(ctx, 42, videoID, domaininteraction.ActionTypeLike, true, "idem-1", &domaininteraction.VideoStat{
		VideoID:       videoID,
		LikeCount:     10,
		CommentCount:  3,
		FavoriteCount: 2,
	})
	if err != nil {
		t.Fatalf("SetActionState like: %v", err)
	}
	if !res.Active || res.Delta != 1 || res.LikeCount != 11 || res.FavoriteCount != 2 {
		t.Fatalf("unexpected like result: %+v", res)
	}

	// 2. 评论数同步（假设 MySQL 中 like_count 尚未落库仍为 10）：
	// 单 Hash 更新 comment_count=4，不得覆盖 Redis 中已领先的 like_count=11。
	if err := cache.SetVideoStat(ctx, &domaininteraction.VideoStat{
		VideoID:       videoID,
		LikeCount:     10,
		CommentCount:  4,
		FavoriteCount: 2,
	}); err != nil {
		t.Fatalf("SetVideoStat comment sync: %v", err)
	}

	stats, err := cache.GetStats(ctx, []int64{videoID})
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}
	stat := stats[videoID]
	if stat == nil || stat.LikeCount != 11 || stat.CommentCount != 4 || stat.FavoriteCount != 2 {
		t.Fatalf("expected like=11 comment=4 fav=2, got %+v", stat)
	}

	// 3. 再次点赞（另一用户），验证 comment_count 不会回退为旧值 3。
	res2, err := cache.SetActionState(ctx, 43, videoID, domaininteraction.ActionTypeLike, true, "idem-2", &domaininteraction.VideoStat{
		VideoID:       videoID,
		LikeCount:     10,
		CommentCount:  3,
		FavoriteCount: 2,
	})
	if err != nil {
		t.Fatalf("SetActionState second user: %v", err)
	}
	if res2.LikeCount != 12 {
		t.Fatalf("expected like=12, got %+v", res2)
	}
	statsAfter, _ := cache.GetStats(ctx, []int64{videoID})
	if statsAfter[videoID].CommentCount != 4 {
		t.Fatalf("comment_count regressed after subsequent like: %+v", statsAfter[videoID])
	}
}

func TestRouteALuaRepeatLikeAfterCacheEvictionDoesNotDoubleCount(t *testing.T) {
	ctx := context.Background()
	redisClient := newFakeRedisClient()
	cache := NewFeedCache(redisClient)
	videoID := int64(2002)

	// Redis 行为键缺失，但 DB 回源显示用户已点赞（StatusActive）：再次点赞 delta 必须为 0。
	existingDBAction := &domaininteraction.Action{
		UserID:     42,
		VideoID:    videoID,
		ActionType: domaininteraction.ActionTypeLike,
		Status:     domaininteraction.ActionStatusActive,
	}
	res, err := cache.SetActionState(ctx, 42, videoID, domaininteraction.ActionTypeLike, true, "idem-repeat", &domaininteraction.VideoStat{
		VideoID:   videoID,
		LikeCount: 5,
	}, existingDBAction)
	if err != nil {
		t.Fatalf("SetActionState repeat like: %v", err)
	}
	if !res.Active || res.Delta != 0 || res.LikeCount != 5 {
		t.Fatalf("expected idempotent no-op like=5 delta=0, got %+v", res)
	}
}

func TestGetViewerActionStatesReadsLiveCache(t *testing.T) {
	ctx := context.Background()
	redisClient := newFakeRedisClient()
	cache := NewFeedCache(redisClient)

	_, err := cache.SetActionState(ctx, 42, 10, domaininteraction.ActionTypeLike, true, "k1", &domaininteraction.VideoStat{VideoID: 10})
	if err != nil {
		t.Fatalf("SetActionState like: %v", err)
	}
	_, err = cache.SetActionState(ctx, 42, 11, domaininteraction.ActionTypeFavorite, false, "k2", &domaininteraction.VideoStat{VideoID: 11})
	if err != nil {
		t.Fatalf("SetActionState unfavorite: %v", err)
	}

	states, err := cache.GetViewerActionStates(ctx, 42, []int64{10, 11, 12})
	if err != nil {
		t.Fatalf("GetViewerActionStates: %v", err)
	}
	if !states[10].HasLike || !states[10].Liked || states[10].HasFavorite {
		t.Fatalf("unexpected video 10 state: %+v", states[10])
	}
	if !states[11].HasFavorite || states[11].Favorited || states[11].HasLike {
		t.Fatalf("unexpected video 11 state: %+v", states[11])
	}
	if _, exists := states[12]; exists {
		t.Fatalf("expected video 12 cache miss, got %+v", states[12])
	}
}

func TestListFollowingIndexPreservesSmallCreatorsAndCleansUnfollowed(t *testing.T) {
	ctx := context.Background()
	redisClient := newFakeRedisClient()
	cache := NewFeedCache(redisClient)

	base := time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)
	// 小作者 200（仅在 inbox，不在大 V pullAuthorIDs）、已取关作者 300（在 inbox 残留）、大 V 100（在 outbox）
	if err := cache.AddInboxItemsBatch(ctx, 200, []int64{42}, []*domainfeed.FeedPageItem{
		{VideoID: 2, AuthorID: 200, PublishedAt: base.Add(-1 * time.Minute)},
	}, 1000); err != nil {
		t.Fatalf("AddInboxItemsBatch small creator: %v", err)
	}
	if err := cache.AddInboxItems(ctx, 300, []int64{42}, &domainfeed.FeedPageItem{
		VideoID: 3, AuthorID: 300, PublishedAt: base,
	}, 1000); err != nil {
		t.Fatalf("AddInboxItems unfollowed: %v", err)
	}
	if err := cache.AddAuthorOutboxItem(ctx, 100, &domainfeed.FeedPageItem{
		VideoID: 1, AuthorID: 100, PublishedAt: base.Add(-2 * time.Minute),
	}, 500); err != nil {
		t.Fatalf("AddAuthorOutboxItem big creator: %v", err)
	}

	// pullAuthorIDs 只包含大 V 100，followedAuthorIDs 包含全量关注 [100, 200]
	items, ok, err := cache.ListFollowingIndexPage(ctx, 42, []int64{100}, []int64{100, 200}, nil, 10)
	if err != nil {
		t.Fatalf("ListFollowingIndexPage: %v", err)
	}
	if !ok || len(items) != 2 {
		t.Fatalf("expected 2 items (small creator 200 + big creator 100), got ok=%v items=%+v", ok, items)
	}
	if items[0].VideoID != 2 || items[1].VideoID != 1 {
		t.Fatalf("unexpected order: %+v", items)
	}

	// 等待异步批量清理 goroutine 删除取关作者 300 的残留，并确认小作者 200 未被误删。
	time.Sleep(50 * time.Millisecond)
	redisClient.mu.Lock()
	removed := append([]string(nil), redisClient.zremLog[followingInboxKey(42)]...)
	remainingInbox := len(redisClient.zsets[followingInboxKey(42)])
	redisClient.mu.Unlock()
	if len(removed) != 1 || !strings.HasPrefix(removed[0], "3:300:") {
		t.Fatalf("expected only unfollowed author 300 removed, got %+v", removed)
	}
	if remainingInbox != 1 {
		t.Fatalf("expected small creator item preserved in inbox, remaining=%d", remainingInbox)
	}
}

func TestListFollowingIndexSameTimestampCursorDoesNotSkipItems(t *testing.T) {
	ctx := context.Background()
	redisClient := newFakeRedisClient()
	cache := NewFeedCache(redisClient)

	sameTime := time.Date(2026, 6, 1, 12, 0, 0, 500000000, time.UTC)
	if err := cache.AddInboxItemsBatch(ctx, 200, []int64{42}, []*domainfeed.FeedPageItem{
		{VideoID: 20, AuthorID: 200, PublishedAt: sameTime},
		{VideoID: 10, AuthorID: 200, PublishedAt: sameTime},
	}, 1000); err != nil {
		t.Fatalf("AddInboxItemsBatch: %v", err)
	}

	firstPage, ok, err := cache.ListFollowingIndexPage(ctx, 42, nil, []int64{200}, nil, 1)
	if err != nil || !ok || len(firstPage) != 1 || firstPage[0].VideoID != 20 {
		t.Fatalf("unexpected first page: ok=%v err=%v items=%+v", ok, err, firstPage)
	}

	cursor := &domainfeed.TimelineCursor{PublishedAt: sameTime, VideoID: 20}
	secondPage, ok, err := cache.ListFollowingIndexPage(ctx, 42, nil, []int64{200}, cursor, 1)
	if err != nil || !ok || len(secondPage) != 1 || secondPage[0].VideoID != 10 {
		t.Fatalf("expected video 10 on second page with same timestamp, got ok=%v err=%v items=%+v", ok, err, secondPage)
	}
}
