package infracache

import (
	applicationfeed "GCFeed/internal/application/feed"
	applicationinteraction "GCFeed/internal/application/interaction"
	domainfeed "GCFeed/internal/domain/feed"
	domaininteraction "GCFeed/internal/domain/interaction"
	inframetrics "GCFeed/internal/infra/metrics"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const hotWindowMinutes = 60
const hotMinuteBucketTTL = 2 * time.Hour
const hotWindowCacheTTL = 2 * time.Minute
const actionStateTTL = 30 * 24 * time.Hour
const actionStatTTL = 24 * time.Hour
const commentStatDefaultTTL = 15 * time.Second
const followingIndexKeyTTL = 30 * 24 * time.Hour

// setActionStateScript 在 Redis 单线程内原子执行：幂等校验 -> 状态切换 -> 单 Hash 计数更新，
// 替代容易冲突的 WATCH 乐观锁与多套缓存（base + shard + json）结构。
var setActionStateScript = redis.NewScript(`
local actionKey = KEYS[1]
local statKey = KEYS[2]

local targetStatus = tonumber(ARGV[1])
local idemKey = ARGV[2]
local updatedAt = ARGV[3]
local actionTTL = tonumber(ARGV[4])
local statTTL = tonumber(ARGV[5])
local field = ARGV[6]
local initLike = tonumber(ARGV[7]) or 0
local initComment = tonumber(ARGV[8]) or 0
local initFav = tonumber(ARGV[9]) or 0
local hasInitAction = tonumber(ARGV[10]) or 0
local initActionActive = tonumber(ARGV[11]) or 0

local storedStatusStr = redis.call("HGET", actionKey, "status")
local storedIdem = redis.call("HGET", actionKey, "idempotency_key") or ""

local storedStatus = 0
if storedStatusStr then
	storedStatus = tonumber(storedStatusStr) or 0
end

local effectiveStatus = targetStatus
local delta = 0

if idemKey ~= "" and storedIdem == idemKey and storedStatus ~= 0 then
	effectiveStatus = storedStatus
	delta = 0
else
	if storedStatus == 0 then
		if hasInitAction == 1 then
			if initActionActive == 1 and targetStatus ~= 1 then
				delta = -1
			elseif initActionActive == 0 and targetStatus == 1 then
				delta = 1
			end
		elseif targetStatus == 1 then
			delta = 1
		end
	elseif storedStatus ~= targetStatus then
		if targetStatus == 1 then
			delta = 1
		else
			delta = -1
		end
	end
end

redis.call("HSET", actionKey,
	"status", tostring(effectiveStatus),
	"idempotency_key", idemKey,
	"updated_at", updatedAt
)
redis.call("EXPIRE", actionKey, actionTTL)

if redis.call("EXISTS", statKey) == 0 then
	redis.call("HSET", statKey,
		"like_count", tostring(initLike),
		"comment_count", tostring(initComment),
		"favorite_count", tostring(initFav)
	)
end

if delta ~= 0 then
	local cur = tonumber(redis.call("HGET", statKey, field) or "0") or 0
	local nextVal = cur + delta
	if nextVal < 0 then
		nextVal = 0
	end
	redis.call("HSET", statKey, field, tostring(nextVal))
end

local curTTL = redis.call("TTL", statKey)
if curTTL < statTTL then
	redis.call("EXPIRE", statKey, statTTL)
end

local likeCount = tonumber(redis.call("HGET", statKey, "like_count") or "0") or 0
local commentCount = tonumber(redis.call("HGET", statKey, "comment_count") or "0") or 0
local favCount = tonumber(redis.call("HGET", statKey, "favorite_count") or "0") or 0

return {effectiveStatus, delta, likeCount, commentCount, favCount}
`)

// syncCommentStatScript 在评论增删时安全同步单 Hash 计数：
// 若 statKey 已存在，仅更新 comment_count，保留未落库的异步 like_count/favorite_count 增量。
var syncCommentStatScript = redis.NewScript(`
local statKey = KEYS[1]
local likeCount = tonumber(ARGV[1]) or 0
local commentCount = tonumber(ARGV[2]) or 0
local favCount = tonumber(ARGV[3]) or 0
local statTTL = tonumber(ARGV[4]) or 15

if redis.call("EXISTS", statKey) == 1 then
	redis.call("HSET", statKey, "comment_count", tostring(commentCount))
	local curTTL = redis.call("TTL", statKey)
	if curTTL >= 0 and curTTL < statTTL then
		redis.call("EXPIRE", statKey, statTTL)
	end
else
	redis.call("HSET", statKey,
		"like_count", tostring(likeCount),
		"comment_count", tostring(commentCount),
		"favorite_count", tostring(favCount)
	)
	redis.call("EXPIRE", statKey, statTTL)
end
return 1
`)

type redisFeedClient interface {
	redis.Cmdable
	redis.Scripter
	Pipeline() redis.Pipeliner
}

// FeedCache 使用 Redis 保存 Feed 页、卡片、单 Hash 实时互动计数与关注流索引。
type FeedCache struct {
	client redisFeedClient
}

// NewFeedCache 创建 Feed 结果缓存。
func NewFeedCache(client redisFeedClient) *FeedCache {
	return &FeedCache{client: client}
}

// GetPage 读取缓存中的轻量 Feed 页。
func (c *FeedCache) GetPage(ctx context.Context, key string) (*applicationfeed.FeedPage, bool, error) {
	content, err := c.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		inframetrics.ObserveCacheRead("page", 1, 0, nil)
		return nil, false, nil
	}
	if err != nil {
		inframetrics.ObserveCacheRead("page", 1, 0, err)
		return nil, false, err
	}

	var page applicationfeed.FeedPage
	if err := json.Unmarshal(content, &page); err != nil {
		inframetrics.ObserveCacheRead("page", 1, 0, err)
		return nil, false, err
	}
	inframetrics.ObserveCacheRead("page", 1, 1, nil)
	return &page, true, nil
}

// SetPage 写入轻量 Feed 页，并设置过期时间。
func (c *FeedCache) SetPage(ctx context.Context, key string, page *applicationfeed.FeedPage, ttl time.Duration) error {
	content, err := json.Marshal(page)
	if err != nil {
		inframetrics.ObserveCacheWrite("page", 1, err)
		return err
	}
	err = c.client.Set(ctx, key, content, ttl).Err()
	inframetrics.ObserveCacheWrite("page", 1, err)
	return err
}

// GetCards 批量读取视频卡片缓存。
func (c *FeedCache) GetCards(ctx context.Context, videoIDs []int64) (map[int64]*domainfeed.FeedCard, error) {
	cards := map[int64]*domainfeed.FeedCard{}
	if len(videoIDs) == 0 {
		return cards, nil
	}

	values, err := c.client.MGet(ctx, cacheKeys(videoIDs, feedCardKey)...).Result()
	if err != nil {
		inframetrics.ObserveCacheRead("card", len(videoIDs), 0, err)
		return nil, err
	}
	for index, value := range values {
		content, ok := cacheValueBytes(value)
		if !ok {
			continue
		}
		var card domainfeed.FeedCard
		if err := json.Unmarshal(content, &card); err != nil {
			continue
		}
		if card.VideoID <= 0 {
			card.VideoID = videoIDs[index]
		}
		cards[card.VideoID] = &card
	}
	inframetrics.ObserveCacheRead("card", len(videoIDs), len(cards), nil)
	return cards, nil
}

// SetCards 批量写入视频卡片缓存。
func (c *FeedCache) SetCards(ctx context.Context, cards map[int64]*domainfeed.FeedCard, ttl time.Duration) error {
	pipe := c.client.Pipeline()
	queued := false

	for _, card := range cards {
		if card == nil || card.VideoID <= 0 {
			continue
		}
		content, err := json.Marshal(card)
		if err != nil {
			return err
		}
		pipe.Set(ctx, feedCardKey(card.VideoID), content, ttl)
		queued = true
	}
	if !queued {
		return nil
	}
	_, err := pipe.Exec(ctx)
	inframetrics.ObserveCacheWrite("card", len(cards), err)
	return err
}

// DeleteCard 删除指定视频的卡片缓存，用于视频删除等场景的缓存失效闭环。
func (c *FeedCache) DeleteCard(ctx context.Context, videoID int64) error {
	if videoID <= 0 {
		return nil
	}
	err := c.client.Del(ctx, feedCardKey(videoID)).Err()
	inframetrics.ObserveCacheWrite("card_delete", 1, err)
	return err
}

// GetStats 通过单次 Pipeline 批量读取单 Hash 互动计数缓存（video:stat:v2:{id}）。
func (c *FeedCache) GetStats(ctx context.Context, videoIDs []int64) (map[int64]*domainfeed.FeedStat, error) {
	stats := map[int64]*domainfeed.FeedStat{}
	if len(videoIDs) == 0 {
		return stats, nil
	}

	pipe := c.client.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, 0, len(videoIDs))
	for _, videoID := range videoIDs {
		cmds = append(cmds, pipe.HGetAll(ctx, feedStatHashKey(videoID)))
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		inframetrics.ObserveCacheRead("stat", len(videoIDs), 0, err)
		return nil, err
	}

	for index, cmd := range cmds {
		values, err := cmd.Result()
		if err != nil && err != redis.Nil {
			inframetrics.ObserveCacheRead("stat", len(videoIDs), 0, err)
			return nil, err
		}
		if len(values) == 0 {
			continue
		}
		stat := &domainfeed.FeedStat{VideoID: videoIDs[index]}
		applyActionStatFields(stat, values)
		stats[stat.VideoID] = stat
	}
	inframetrics.ObserveCacheRead("stat", len(videoIDs), len(stats), nil)
	return stats, nil
}

// SetStats 使用 HSetNX 批量回填缺失的视频计数 Hash，避免并发回源覆盖刚写入的异步点赞增量。
func (c *FeedCache) SetStats(ctx context.Context, stats map[int64]*domainfeed.FeedStat, ttl time.Duration) error {
	pipe := c.client.Pipeline()
	queued := false

	for _, stat := range stats {
		if stat == nil || stat.VideoID <= 0 {
			continue
		}
		key := feedStatHashKey(stat.VideoID)
		pipe.HSetNX(ctx, key, "like_count", clampRedisCount(stat.LikeCount))
		pipe.HSetNX(ctx, key, "comment_count", clampRedisCount(stat.CommentCount))
		pipe.HSetNX(ctx, key, "favorite_count", clampRedisCount(stat.FavoriteCount))
		if ttl > 0 {
			pipe.Expire(ctx, key, ttl)
		}
		queued = true
	}
	if !queued {
		return nil
	}
	_, err := pipe.Exec(ctx)
	inframetrics.ObserveCacheWrite("stat", len(stats), err)
	return err
}

// SetVideoStat 同步单个视频的评论数到单 Hash 缓存：若 Hash 已存在则只更新 comment_count，不覆盖异步点赞/收藏计数。
func (c *FeedCache) SetVideoStat(ctx context.Context, stat *domaininteraction.VideoStat) error {
	if stat == nil || stat.VideoID <= 0 {
		return nil
	}
	err := syncCommentStatScript.Run(
		ctx,
		c.client,
		[]string{feedStatHashKey(stat.VideoID)},
		clampRedisCount(stat.LikeCount),
		clampRedisCount(stat.CommentCount),
		clampRedisCount(stat.FavoriteCount),
		int64(commentStatDefaultTTL.Seconds()),
	).Err()
	inframetrics.ObserveCacheWrite("stat", 1, err)
	return err
}

// HasActionState 检查指定用户的点赞/收藏状态是否已存在于 Redis 缓存中，便于应用层按需回源 DB。
func (c *FeedCache) HasActionState(ctx context.Context, userID int64, videoID int64, actionType string) (bool, error) {
	normalized, err := domaininteraction.NormalizeActionType(actionType)
	if err != nil {
		return false, err
	}
	exists, err := c.client.Exists(ctx, interactionActionKey(userID, videoID, normalized)).Result()
	if err != nil {
		return false, err
	}
	return exists > 0, nil
}

// GetViewerActionStates 批量读取用户在 Redis 中的实时点赞/收藏状态，保障异步写后立即可见的读己之写一致性。
func (c *FeedCache) GetViewerActionStates(ctx context.Context, viewerID int64, videoIDs []int64) (map[int64]applicationfeed.CachedViewerActionState, error) {
	states := map[int64]applicationfeed.CachedViewerActionState{}
	if viewerID <= 0 || len(videoIDs) == 0 {
		return states, nil
	}

	pipe := c.client.Pipeline()
	likeCmds := make([]*redis.StringCmd, 0, len(videoIDs))
	favCmds := make([]*redis.StringCmd, 0, len(videoIDs))
	for _, videoID := range videoIDs {
		likeCmds = append(likeCmds, pipe.HGet(ctx, interactionActionKey(viewerID, videoID, domaininteraction.ActionTypeLike), "status"))
		favCmds = append(favCmds, pipe.HGet(ctx, interactionActionKey(viewerID, videoID, domaininteraction.ActionTypeFavorite), "status"))
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, err
	}

	for index, videoID := range videoIDs {
		var entry applicationfeed.CachedViewerActionState
		if raw, err := likeCmds[index].Result(); err == nil {
			if status, parseErr := strconv.Atoi(raw); parseErr == nil && status > 0 {
				entry.HasLike = true
				entry.Liked = status == domaininteraction.ActionStatusActive
			}
		} else if err != redis.Nil {
			return nil, err
		}
		if raw, err := favCmds[index].Result(); err == nil {
			if status, parseErr := strconv.Atoi(raw); parseErr == nil && status > 0 {
				entry.HasFavorite = true
				entry.Favorited = status == domaininteraction.ActionStatusActive
			}
		} else if err != redis.Nil {
			return nil, err
		}
		if entry.HasLike || entry.HasFavorite {
			states[videoID] = entry
		}
	}
	return states, nil
}

// SetActionState 通过单条 Redis Lua 脚本原子写入用户行为状态和单 Hash 实时计数（路线 A）。
func (c *FeedCache) SetActionState(ctx context.Context, userID int64, videoID int64, actionType string, active bool, idempotencyKey string, initialStat *domaininteraction.VideoStat, initialAction ...*domaininteraction.Action) (*applicationinteraction.ActionStateResult, error) {
	actionType, err := domaininteraction.NormalizeActionType(actionType)
	if err != nil {
		return nil, err
	}
	idempotencyKey = strings.TrimSpace(idempotencyKey)

	actionKey := interactionActionKey(userID, videoID, actionType)
	statKey := feedStatHashKey(videoID)
	targetStatus := domaininteraction.ActionStatusCanceled
	if active {
		targetStatus = domaininteraction.ActionStatusActive
	}

	initLike, initComment, initFav := 0, 0, 0
	if initialStat != nil {
		initLike = clampRedisCount(initialStat.LikeCount)
		initComment = clampRedisCount(initialStat.CommentCount)
		initFav = clampRedisCount(initialStat.FavoriteCount)
	}
	hasInitAction := 0
	initActionActive := 0
	if len(initialAction) > 0 && initialAction[0] != nil {
		hasInitAction = 1
		if initialAction[0].Active() {
			initActionActive = 1
		}
	}

	raw, err := setActionStateScript.Run(
		ctx,
		c.client,
		[]string{actionKey, statKey},
		targetStatus,
		idempotencyKey,
		time.Now().UTC().Format(time.RFC3339Nano),
		int64(actionStateTTL.Seconds()),
		int64(actionStatTTL.Seconds()),
		interactionStatField(actionType),
		initLike,
		initComment,
		initFav,
		hasInitAction,
		initActionActive,
	).Slice()
	if err != nil {
		return nil, err
	}
	if len(raw) < 5 {
		return nil, errors.New("unexpected lua action state response")
	}

	effectiveStatus := sliceInt(raw[0])
	delta := sliceInt(raw[1])
	likeCount := clampRedisCount(sliceInt(raw[2]))
	favCount := clampRedisCount(sliceInt(raw[4]))

	return &applicationinteraction.ActionStateResult{
		VideoID:        videoID,
		ActionType:     actionType,
		Active:         effectiveStatus == domaininteraction.ActionStatusActive,
		LikeCount:      likeCount,
		FavoriteCount:  favCount,
		Delta:          delta,
		IdempotencyKey: idempotencyKey,
	}, nil
}

func (c *FeedCache) AddInboxItems(ctx context.Context, authorID int64, userIDs []int64, item *domainfeed.FeedPageItem, maxLen int64) error {
	if item == nil {
		return nil
	}
	return c.AddInboxItemsBatch(ctx, authorID, userIDs, []*domainfeed.FeedPageItem{item}, maxLen)
}

// AddInboxItemsBatch 使用单次 Pipeline 批量将多条视频写入粉丝 inbox，避免关注回填产生 N 次 RTT。
func (c *FeedCache) AddInboxItemsBatch(ctx context.Context, authorID int64, userIDs []int64, items []*domainfeed.FeedPageItem, maxLen int64) error {
	if authorID <= 0 || len(userIDs) == 0 || len(items) == 0 {
		return nil
	}
	if maxLen <= 0 {
		maxLen = 1000
	}

	members := make([]redis.Z, 0, len(items))
	for _, item := range items {
		if item == nil || item.VideoID <= 0 || item.PublishedAt.IsZero() {
			continue
		}
		members = append(members, redis.Z{
			Score:  followingIndexScore(item.PublishedAt, item.VideoID),
			Member: followingIndexMember(item.VideoID, authorID, item.PublishedAt),
		})
	}
	if len(members) == 0 {
		return nil
	}

	pipe := c.client.Pipeline()
	queued := false
	for _, userID := range userIDs {
		if userID <= 0 {
			continue
		}
		key := followingInboxKey(userID)
		pipe.ZAdd(ctx, key, members...)
		pipe.ZRemRangeByRank(ctx, key, 0, -maxLen-1)
		pipe.Expire(ctx, key, followingIndexKeyTTL)
		queued = true
	}
	if !queued {
		return nil
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (c *FeedCache) AddAuthorOutboxItem(ctx context.Context, authorID int64, item *domainfeed.FeedPageItem, maxLen int64) error {
	if authorID <= 0 || item == nil || item.VideoID <= 0 || item.PublishedAt.IsZero() {
		return nil
	}
	if maxLen <= 0 {
		maxLen = 500
	}
	key := followingAuthorOutboxKey(authorID)
	score := followingIndexScore(item.PublishedAt, item.VideoID)
	member := followingIndexMember(item.VideoID, authorID, item.PublishedAt)
	pipe := c.client.Pipeline()
	pipe.ZAdd(ctx, key, redis.Z{Score: score, Member: member})
	pipe.ZRemRangeByRank(ctx, key, 0, -maxLen-1)
	pipe.Expire(ctx, key, followingIndexKeyTTL)
	_, err := pipe.Exec(ctx)
	return err
}

// ListFollowingIndexPage 合并用户 inbox 与大 V author outbox：
// pullAuthorIDs 仅用于拉取大 V 的 outbox，followedAuthorIDs 为当前全部已关注作者，用于校验并批量清理 inbox 取关残留。
func (c *FeedCache) ListFollowingIndexPage(ctx context.Context, viewerID int64, pullAuthorIDs []int64, followedAuthorIDs []int64, cursor *domainfeed.TimelineCursor, limit int) ([]*domainfeed.FeedPageItem, bool, error) {
	if viewerID <= 0 || limit <= 0 {
		return []*domainfeed.FeedPageItem{}, false, nil
	}
	keys := []string{followingInboxKey(viewerID)}
	for _, authorID := range pullAuthorIDs {
		if authorID > 0 {
			keys = append(keys, followingAuthorOutboxKey(authorID))
		}
	}

	pipe := c.client.Pipeline()
	cardinalityCommands := make([]*redis.IntCmd, 0, len(keys))
	rangeCommands := make([]*redis.StringSliceCmd, 0, len(keys))
	minScore := "-inf"
	maxScore := "+inf"
	if cursor != nil {
		// 使用闭区间拉取并由应用层按 (PublishedAt, VideoID) 精确过滤，避免同毫秒或大 ID 分数冲突导致漏项。
		maxScore = strconv.FormatFloat(followingIndexScore(cursor.PublishedAt, cursor.VideoID), 'f', 0, 64)
	}
	fetchCount := int64(limit*2 + 10)
	for _, key := range keys {
		cardinalityCommands = append(cardinalityCommands, pipe.ZCard(ctx, key))
		rangeCommands = append(rangeCommands, pipe.ZRevRangeByScore(ctx, key, &redis.ZRangeBy{
			Min:   minScore,
			Max:   maxScore,
			Count: fetchCount,
		}))
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, false, err
	}

	hasIndex := false
	for _, cmd := range cardinalityCommands {
		count, err := cmd.Result()
		if err != nil && err != redis.Nil {
			return nil, false, err
		}
		if count > 0 {
			hasIndex = true
			break
		}
	}
	if !hasIndex {
		return nil, false, nil
	}

	seen := map[int64]struct{}{}
	allowedAuthors := int64Set(followedAuthorIDs)
	for _, authorID := range pullAuthorIDs {
		allowedAuthors[authorID] = struct{}{}
	}
	checkFollowed := followedAuthorIDs != nil
	staleInboxMembers := make([]any, 0)
	items := make([]*domainfeed.FeedPageItem, 0, limit*len(rangeCommands))
	for commandIndex, cmd := range rangeCommands {
		members, err := cmd.Result()
		if err != nil && err != redis.Nil {
			return nil, false, err
		}
		for _, member := range members {
			item, ok := feedPageItemFromFollowingMember(member)
			if !ok {
				continue
			}
			// 针对 inbox 校验全量关注关系，取关作者的内容不再展示并合并批量异步清理。
			if checkFollowed && item.AuthorID > 0 {
				if _, followed := allowedAuthors[item.AuthorID]; !followed {
					if commandIndex == 0 {
						staleInboxMembers = append(staleInboxMembers, member)
					}
					continue
				}
			}
			// 应用层依据 (PublishedAt, VideoID) 全序过滤游标，杜绝分数截断/冲突造成的漏项或重复。
			if cursor != nil {
				if !item.PublishedAt.Before(cursor.PublishedAt) &&
					!(item.PublishedAt.Equal(cursor.PublishedAt) && item.VideoID < cursor.VideoID) {
					continue
				}
			}
			if _, exists := seen[item.VideoID]; exists {
				continue
			}
			seen[item.VideoID] = struct{}{}
			items = append(items, item)
		}
	}

	if len(staleInboxMembers) > 0 {
		inboxKey := followingInboxKey(viewerID)
		go func(key string, stale []any) {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
			defer cancel()
			_ = c.client.ZRem(cleanupCtx, key, stale...).Err()
		}(inboxKey, staleInboxMembers)
	}

	sortFeedPageItemsByTimeline(items)
	if len(items) > limit {
		items = items[:limit]
	}
	return items, true, nil
}

// AddHotScore 把一次互动热度写入 1 分钟粒度的热榜桶。
func (c *FeedCache) AddHotScore(ctx context.Context, videoID int64, scoreDelta int, at time.Time) error {
	if videoID <= 0 || scoreDelta == 0 {
		return nil
	}

	key := hotMinuteKey(at)
	if err := c.client.ZIncrBy(ctx, key, float64(scoreDelta), hotRankMember(videoID)).Err(); err != nil {
		return err
	}
	return c.client.Expire(ctx, key, hotMinuteBucketTTL).Err()
}

// ListHotWindowPage 合并最近 60 个分钟桶，返回一小时滑动窗口内的热榜页。
func (c *FeedCache) ListHotWindowPage(ctx context.Context, windowEnd time.Time, offset int, limit int) ([]*domainfeed.FeedPageItem, error) {
	items := []*domainfeed.FeedPageItem{}
	if limit <= 0 {
		return items, nil
	}
	if offset < 0 {
		offset = 0
	}

	windowEnd = windowEnd.UTC().Truncate(time.Minute)
	windowKey := hotWindowKey(windowEnd)
	exists, err := c.client.Exists(ctx, windowKey).Result()
	if err != nil {
		return nil, err
	}
	if exists == 0 {
		if err := c.rebuildHotWindow(ctx, windowKey, windowEnd); err != nil {
			return nil, err
		}
	}

	return c.listHotWindowPage(ctx, windowKey, offset, limit)
}

func (c *FeedCache) rebuildHotWindow(ctx context.Context, windowKey string, windowEnd time.Time) error {
	if _, err := c.client.ZUnionStore(ctx, windowKey, &redis.ZStore{
		Keys:      hotWindowMinuteKeys(windowEnd),
		Aggregate: "SUM",
	}).Result(); err != nil {
		return err
	}

	pipe := c.client.Pipeline()
	pipe.ZRemRangeByScore(ctx, windowKey, "-inf", "0")
	pipe.Expire(ctx, windowKey, hotWindowCacheTTL)
	_, err := pipe.Exec(ctx)
	return err
}

func (c *FeedCache) listHotWindowPage(ctx context.Context, windowKey string, offset int, limit int) ([]*domainfeed.FeedPageItem, error) {
	items := []*domainfeed.FeedPageItem{}
	values, err := c.client.ZRevRangeWithScores(ctx, windowKey, int64(offset), int64(offset+limit-1)).Result()
	if err != nil {
		return nil, err
	}
	for _, value := range values {
		member, ok := value.Member.(string)
		if !ok {
			continue
		}
		videoID, ok := hotRankVideoID(member)
		if !ok {
			continue
		}
		items = append(items, &domainfeed.FeedPageItem{
			VideoID:  videoID,
			HotScore: int(value.Score),
		})
	}
	return items, nil
}

func applyActionStatFields(stat *domainfeed.FeedStat, values map[string]string) {
	if stat == nil {
		return
	}
	stat.LikeCount = clampRedisCount(actionStatFieldInt(values, "like_count"))
	stat.CommentCount = clampRedisCount(actionStatFieldInt(values, "comment_count"))
	stat.FavoriteCount = clampRedisCount(actionStatFieldInt(values, "favorite_count"))
}

func actionStatFieldInt(values map[string]string, field string) int {
	value, _ := strconv.Atoi(values[field])
	return value
}

func sliceInt(value any) int {
	switch typed := value.(type) {
	case int64:
		return int(typed)
	case int:
		return typed
	case string:
		parsed, _ := strconv.Atoi(typed)
		return parsed
	default:
		return 0
	}
}

func cacheKeys(videoIDs []int64, build func(int64) string) []string {
	keys := make([]string, 0, len(videoIDs))
	for _, videoID := range videoIDs {
		keys = append(keys, build(videoID))
	}
	return keys
}

func feedCardKey(videoID int64) string {
	return fmt.Sprintf("video:card:v1:%d", videoID)
}

func feedStatHashKey(videoID int64) string {
	return fmt.Sprintf("video:stat:v2:%d", videoID)
}

func followingInboxKey(userID int64) string {
	return fmt.Sprintf("feed:following:inbox:v1:%d", userID)
}

func followingAuthorOutboxKey(authorID int64) string {
	return fmt.Sprintf("feed:following:author:v1:%d", authorID)
}

// followingIndexScore 保留毫秒级时间戳精度（2026 年约为 1.79e12，在 IEEE-754 float64 53 位精确整数范围内）。
func followingIndexScore(publishedAt time.Time, videoID int64) float64 {
	return float64(publishedAt.UTC().UnixMilli())
}

func followingIndexMember(videoID int64, authorID int64, publishedAt time.Time) string {
	return fmt.Sprintf("%d:%d:%s", videoID, authorID, publishedAt.UTC().Format(time.RFC3339Nano))
}

func feedPageItemFromFollowingMember(member string) (*domainfeed.FeedPageItem, bool) {
	parts := strings.SplitN(member, ":", 3)
	if len(parts) != 2 && len(parts) != 3 {
		return nil, false
	}
	videoID, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || videoID <= 0 {
		return nil, false
	}
	authorID := int64(0)
	publishedAtIndex := 1
	if len(parts) == 3 {
		authorID, _ = strconv.ParseInt(parts[1], 10, 64)
		publishedAtIndex = 2
	}
	publishedAt, err := time.Parse(time.RFC3339Nano, parts[publishedAtIndex])
	if err != nil || publishedAt.IsZero() {
		return nil, false
	}
	return &domainfeed.FeedPageItem{
		VideoID:     videoID,
		AuthorID:    authorID,
		PublishedAt: publishedAt,
	}, true
}

func int64Set(values []int64) map[int64]struct{} {
	set := map[int64]struct{}{}
	for _, value := range values {
		set[value] = struct{}{}
	}
	return set
}

func sortFeedPageItemsByTimeline(items []*domainfeed.FeedPageItem) {
	sort.Slice(items, func(i, j int) bool {
		if items[i].PublishedAt.Equal(items[j].PublishedAt) {
			return items[i].VideoID > items[j].VideoID
		}
		return items[i].PublishedAt.After(items[j].PublishedAt)
	})
}

func interactionActionKey(userID int64, videoID int64, actionType string) string {
	return fmt.Sprintf("interaction:action:v1:%d:%d:%s", userID, videoID, strings.ToLower(actionType))
}

func interactionStatField(actionType string) string {
	if actionType == domaininteraction.ActionTypeLike {
		return "like_count"
	}
	return "favorite_count"
}

func clampRedisCount(value int) int {
	if value < 0 {
		return 0
	}
	return value
}

func hotMinuteKey(at time.Time) string {
	return fmt.Sprintf("feed:hot:minute:v1:%s", at.UTC().Truncate(time.Minute).Format("200601021504"))
}

func hotWindowKey(windowEnd time.Time) string {
	return fmt.Sprintf("feed:hot:window:v1:%d", windowEnd.UTC().Truncate(time.Minute).Unix())
}

func hotWindowMinuteKeys(windowEnd time.Time) []string {
	keys := make([]string, 0, hotWindowMinutes)
	for index := hotWindowMinutes - 1; index >= 0; index-- {
		keys = append(keys, hotMinuteKey(windowEnd.Add(-time.Duration(index)*time.Minute)))
	}
	return keys
}

func hotRankMember(videoID int64) string {
	return fmt.Sprintf("%020d", videoID)
}

func hotRankVideoID(member string) (int64, bool) {
	value := strings.TrimLeft(member, "0")
	if value == "" {
		return 0, false
	}
	videoID, err := strconv.ParseInt(value, 10, 64)
	return videoID, err == nil && videoID > 0
}

func cacheValueBytes(value any) ([]byte, bool) {
	switch typed := value.(type) {
	case nil:
		return nil, false
	case string:
		return []byte(typed), true
	case []byte:
		return typed, true
	default:
		return nil, false
	}
}
