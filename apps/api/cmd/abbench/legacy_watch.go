package main

import (
	applicationinteraction "GCFeed/internal/application/interaction"
	domainfeed "GCFeed/internal/domain/feed"
	domaininteraction "GCFeed/internal/domain/interaction"
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	legacyActionStatCounterShardCount = 16
	legacyActionStateTTL              = 30 * 24 * time.Hour
	legacyActionStatTTL               = 24 * time.Hour
	legacyActionStatJSONTTL           = 15 * time.Second
)

// LegacyWatchFeedCache 严格复刻 commit 0e8afc4~1（Lua 升级前）的 WATCH 乐观锁 + 16 分片计数实现，仅用于 A/B 压测对照。
type LegacyWatchFeedCache struct {
	client *redis.Client
}

func NewLegacyWatchFeedCache(client *redis.Client) *LegacyWatchFeedCache {
	return &LegacyWatchFeedCache{client: client}
}

func (c *LegacyWatchFeedCache) HasActionState(ctx context.Context, userID int64, videoID int64, actionType string) (bool, error) {
	normalized, err := domaininteraction.NormalizeActionType(actionType)
	if err != nil {
		return false, err
	}
	exists, err := c.client.Exists(ctx, legacyInteractionActionKey(userID, videoID, normalized)).Result()
	if err != nil {
		return false, err
	}
	return exists > 0, nil
}

func (c *LegacyWatchFeedCache) SetActionState(ctx context.Context, userID int64, videoID int64, actionType string, active bool, idempotencyKey string, initialStat *domaininteraction.VideoStat, initialAction ...*domaininteraction.Action) (*applicationinteraction.ActionStateResult, error) {
	actionType, err := domaininteraction.NormalizeActionType(actionType)
	if err != nil {
		return nil, err
	}
	idempotencyKey = strings.TrimSpace(idempotencyKey)

	actionKey := legacyInteractionActionKey(userID, videoID, actionType)
	counterBaseKey := legacyCounterBaseKey(videoID)
	counterShardKey := legacyCounterShardKey(videoID, int(userID%legacyActionStatCounterShardCount))
	jsonKey := legacyFeedStatJSONKey(videoID)

	targetStatus := domaininteraction.ActionStatusCanceled
	if active {
		targetStatus = domaininteraction.ActionStatusActive
	}

	var result *applicationinteraction.ActionStateResult
	err = c.client.Watch(ctx, func(tx *redis.Tx) error {
		values, err := tx.HGetAll(ctx, actionKey).Result()
		if err != nil {
			return err
		}

		storedStatus, _ := strconv.Atoi(values["status"])
		storedIDKey := values["idempotency_key"]
		effectiveActive := active
		effectiveStatus := targetStatus
		delta := 0
		if storedIDKey == idempotencyKey && idempotencyKey != "" {
			effectiveActive = storedStatus == domaininteraction.ActionStatusActive
			effectiveStatus = storedStatus
			delta = 0
		} else {
			if storedStatus == 0 {
				if len(initialAction) > 0 && initialAction[0] != nil {
					if initialAction[0].Active() {
						if !active {
							delta = -1
						}
					} else if active {
						delta = 1
					}
				} else if active {
					delta = 1
				}
			} else if storedStatus != targetStatus {
				if active {
					delta = 1
				} else {
					delta = -1
				}
			}
		}

		baseStat := &domaininteraction.VideoStat{VideoID: videoID}
		if initialStat != nil {
			baseStat = initialStat
		}

		_, err = tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.HSet(ctx, actionKey, map[string]any{
				"status":          effectiveStatus,
				"idempotency_key": idempotencyKey,
				"updated_at":      time.Now().UTC().Format(time.RFC3339Nano),
			})
			pipe.Expire(ctx, actionKey, legacyActionStateTTL)
			pipe.HSetNX(ctx, counterBaseKey, "like_count", baseStat.LikeCount)
			pipe.HSetNX(ctx, counterBaseKey, "comment_count", baseStat.CommentCount)
			pipe.HSetNX(ctx, counterBaseKey, "favorite_count", baseStat.FavoriteCount)
			pipe.Expire(ctx, counterBaseKey, legacyActionStatTTL)
			if delta != 0 {
				pipe.HIncrBy(ctx, counterShardKey, legacyStatField(actionType), int64(delta))
			}
			for _, shardKey := range legacyCounterShardKeys(videoID) {
				pipe.Expire(ctx, shardKey, legacyActionStatTTL)
			}
			return nil
		})
		if err != nil {
			return err
		}

		result = &applicationinteraction.ActionStateResult{
			VideoID:        videoID,
			ActionType:     actionType,
			Active:         effectiveActive,
			Delta:          delta,
			IdempotencyKey: idempotencyKey,
		}
		return nil
	}, actionKey)
	if err != nil {
		return nil, err
	}

	stat, err := c.readActionStat(ctx, counterBaseKey, legacyCounterShardKeys(videoID), jsonKey, videoID, initialStat)
	if err != nil {
		return nil, err
	}
	result.LikeCount = stat.LikeCount
	result.FavoriteCount = stat.FavoriteCount
	if content, mErr := json.Marshal(stat); mErr == nil {
		_ = c.client.Set(ctx, jsonKey, content, legacyActionStatJSONTTL).Err()
	}
	return result, nil
}

func (c *LegacyWatchFeedCache) readActionStat(ctx context.Context, counterBaseKey string, shardKeys []string, jsonKey string, videoID int64, initialStat *domaininteraction.VideoStat) (*domainfeed.FeedStat, error) {
	stat := &domainfeed.FeedStat{VideoID: videoID}
	values, err := c.client.HGetAll(ctx, counterBaseKey).Result()
	if err != nil {
		return nil, err
	}
	if len(values) > 0 {
		stat.LikeCount, _ = strconv.Atoi(values["like_count"])
		stat.CommentCount, _ = strconv.Atoi(values["comment_count"])
		stat.FavoriteCount, _ = strconv.Atoi(values["favorite_count"])
	} else {
		content, err := c.client.Get(ctx, jsonKey).Bytes()
		if err == redis.Nil {
			if initialStat != nil {
				stat.LikeCount = initialStat.LikeCount
				stat.CommentCount = initialStat.CommentCount
				stat.FavoriteCount = initialStat.FavoriteCount
			}
		} else if err != nil {
			return nil, err
		} else {
			_ = json.Unmarshal(content, stat)
			stat.VideoID = videoID
		}
	}

	pipe := c.client.Pipeline()
	cmds := make([]*redis.MapStringStringCmd, 0, len(shardKeys))
	for _, key := range shardKeys {
		cmds = append(cmds, pipe.HGetAll(ctx, key))
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, err
	}
	likeDelta := 0
	favDelta := 0
	for _, cmd := range cmds {
		vals, err := cmd.Result()
		if err != nil && err != redis.Nil {
			return nil, err
		}
		l, _ := strconv.Atoi(vals["like_count"])
		f, _ := strconv.Atoi(vals["favorite_count"])
		likeDelta += l
		favDelta += f
	}
	stat.LikeCount = max(0, stat.LikeCount+likeDelta)
	stat.FavoriteCount = max(0, stat.FavoriteCount+favDelta)
	return stat, nil
}

func legacyInteractionActionKey(userID, videoID int64, actionType string) string {
	return fmt.Sprintf("interaction:action:v1:%d:%d:%s", userID, videoID, strings.ToLower(actionType))
}

func legacyCounterBaseKey(videoID int64) string {
	return fmt.Sprintf("interaction:stat:counter:v1:%d:base", videoID)
}

func legacyCounterShardKey(videoID int64, shard int) string {
	if shard < 0 {
		shard = 0
	}
	return fmt.Sprintf("interaction:stat:counter:v1:%d:shard:%02d", videoID, shard)
}

func legacyCounterShardKeys(videoID int64) []string {
	keys := make([]string, 0, legacyActionStatCounterShardCount)
	for i := range legacyActionStatCounterShardCount {
		keys = append(keys, legacyCounterShardKey(videoID, i))
	}
	return keys
}

func legacyFeedStatJSONKey(videoID int64) string {
	return fmt.Sprintf("video:stat:v1:%d", videoID)
}

func legacyStatField(actionType string) string {
	if actionType == domaininteraction.ActionTypeLike {
		return "like_count"
	}
	return "favorite_count"
}
