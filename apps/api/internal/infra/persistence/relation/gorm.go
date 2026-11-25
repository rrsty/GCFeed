package infrarelation

import (
	domainaccount "GCFeed/internal/domain/account"
	domainrelation "GCFeed/internal/domain/relation"
	infraaccount "GCFeed/internal/infra/persistence/account"
	"context"
	"errors"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type Repository struct {
	db *gorm.DB
}

type relationUserModel struct {
	UserID     int64
	Nickname   string
	AvatarURL  string
	Bio        string
	FollowedAt time.Time
}

type relationUserProfileModel struct {
	UserID    int64
	Nickname  string
	AvatarURL string
	Bio       string
}

// New 创建关系仓储实现。
func New(db *gorm.DB) *Repository {
	return &Repository{db: db}
}

// SetFollow 设置关注或取关状态，并在同一事务中维护双方计数。
func (r *Repository) SetFollow(ctx context.Context, userID int64, targetUserID int64, active bool, idempotencyKey string) (*domainrelation.Follow, *domainrelation.RelationStat, *domainrelation.RelationStat, error) {
	idempotencyKey = strings.TrimSpace(idempotencyKey)

	var follow FollowModel
	var userStat RelationStatModel
	var targetStat RelationStatModel
	err := r.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		// 使用普通读校验账号有效，不对大 V account 行加排他锁，消除并发互关死锁和热点串行化。
		if err := ensureNormalUser(tx, userID); err != nil {
			return err
		}
		if err := ensureNormalUser(tx, targetUserID); err != nil {
			return err
		}

		// 按升序初始化统计行，保证锁获取顺序一致。
		firstID, secondID := orderedPair(userID, targetUserID)
		if err := ensureStat(tx, firstID); err != nil {
			return err
		}
		if err := ensureStat(tx, secondID); err != nil {
			return err
		}

		findErr := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
			Where("user_id = ? AND target_user_id = ?", userID, targetUserID).
			Take(&follow).
			Error
		if findErr != nil && !errors.Is(findErr, gorm.ErrRecordNotFound) {
			return findErr
		}

		nextStatus := statusFromActive(active)
		delta := 0
		if errors.Is(findErr, gorm.ErrRecordNotFound) {
			follow = FollowModel{
				UserID:         userID,
				TargetUserID:   targetUserID,
				Status:         nextStatus,
				IdempotencyKey: idempotencyKeyPtr(idempotencyKey),
			}
			if err := tx.Create(&follow).Error; err != nil {
				return err
			}
			if active {
				delta = 1
			}
		} else {
			if idempotencyKey != "" && idempotencyKeyValue(follow.IdempotencyKey) == idempotencyKey {
				var err error
				userStat, targetStat, err = loadRelationStatsOrdered(tx, userID, targetUserID)
				return err
			}

			previousStatus := follow.Status
			previousIdempotencyKey := idempotencyKeyValue(follow.IdempotencyKey)
			if follow.Status != nextStatus {
				if active {
					delta = 1
				} else {
					delta = -1
				}
			}
			follow.Status = nextStatus
			follow.IdempotencyKey = idempotencyKeyPtr(idempotencyKey)
			if previousStatus != nextStatus || previousIdempotencyKey != idempotencyKey {
				if err := tx.Save(&follow).Error; err != nil {
					return err
				}
			}
		}

		var err error
		if delta != 0 {
			// 严格按 user_id 升序加锁更新双方统计，彻底杜绝互相关注 AB-BA 死锁。
			userStat, targetStat, err = updateRelationStatsOrdered(tx, userID, targetUserID, delta)
			return err
		}

		userStat, targetStat, err = loadRelationStatsOrdered(tx, userID, targetUserID)
		return err
	})
	if err != nil {
		return nil, nil, nil, mapUserError(err)
	}

	return restoreFollow(follow), restoreStat(userStat), restoreStat(targetStat), nil
}

// ListFollowing 查询当前用户关注的人。
func (r *Repository) ListFollowing(ctx context.Context, userID int64, cursor *domainrelation.ListCursor, limit int) ([]*domainrelation.UserItem, error) {
	query := r.db.WithContext(ctx).
		Table("user_follow AS f").
		Select("a.id AS user_id, a.nickname, a.avatar_url, a.bio, f.updated_at AS followed_at").
		Joins("LEFT JOIN account AS a ON a.id = f.target_user_id").
		Where("f.user_id = ? AND f.status = ? AND a.status = ?", userID, domainrelation.FollowStatusActive, domainaccount.StatusNormal)

	if cursor != nil {
		query = query.Where(
			"(f.updated_at < ? OR (f.updated_at = ? AND f.target_user_id < ?))",
			cursor.FollowedAt,
			cursor.FollowedAt,
			cursor.UserID,
		)
	}

	return scanUserItems(query.Order("f.updated_at DESC").Order("f.target_user_id DESC").Limit(limit))
}

// ListFollowers 查询关注当前用户的人。
func (r *Repository) ListFollowers(ctx context.Context, userID int64, cursor *domainrelation.ListCursor, limit int) ([]*domainrelation.UserItem, error) {
	query := r.db.WithContext(ctx).
		Table("user_follow AS f").
		Select("a.id AS user_id, a.nickname, a.avatar_url, a.bio, f.updated_at AS followed_at").
		Joins("LEFT JOIN account AS a ON a.id = f.user_id").
		Where("f.target_user_id = ? AND f.status = ? AND a.status = ?", userID, domainrelation.FollowStatusActive, domainaccount.StatusNormal)

	if cursor != nil {
		query = query.Where(
			"(f.updated_at < ? OR (f.updated_at = ? AND f.user_id < ?))",
			cursor.FollowedAt,
			cursor.FollowedAt,
			cursor.UserID,
		)
	}

	return scanUserItems(query.Order("f.updated_at DESC").Order("f.user_id DESC").Limit(limit))
}

// GetUserProfile 读取用户展示资料，用于关注通知。
func (r *Repository) GetUserProfile(ctx context.Context, userID int64) (*domainrelation.UserProfile, error) {
	var model relationUserProfileModel
	err := r.db.WithContext(ctx).
		Table("account").
		Select("id AS user_id, nickname, avatar_url, bio").
		Where("id = ? AND status = ?", userID, domainaccount.StatusNormal).
		Take(&model).
		Error
	if err != nil {
		return nil, mapUserError(err)
	}
	return domainrelation.RestoreUserProfile(model.UserID, model.Nickname, model.AvatarURL, model.Bio), nil
}

func scanUserItems(query *gorm.DB) ([]*domainrelation.UserItem, error) {
	var models []relationUserModel
	if err := query.Scan(&models).Error; err != nil {
		return nil, err
	}

	items := make([]*domainrelation.UserItem, 0, len(models))
	for _, model := range models {
		items = append(items, domainrelation.RestoreUserItem(
			model.UserID,
			model.Nickname,
			model.AvatarURL,
			model.Bio,
			model.FollowedAt,
		))
	}
	return items, nil
}

func statusFromActive(active bool) int {
	if active {
		return domainrelation.FollowStatusActive
	}
	return domainrelation.FollowStatusCanceled
}

func ensureNormalUser(tx *gorm.DB, userID int64) error {
	var user infraaccount.UserModel
	err := tx.Select("id").
		Where("id = ? AND status = ?", userID, domainaccount.StatusNormal).
		Take(&user).
		Error
	return mapUserError(err)
}

func orderedPair(left int64, right int64) (int64, int64) {
	if left <= right {
		return left, right
	}
	return right, left
}

func loadRelationStatsOrdered(tx *gorm.DB, userID int64, targetUserID int64) (RelationStatModel, RelationStatModel, error) {
	var userStat, targetStat RelationStatModel
	if userID <= targetUserID {
		var err error
		if userStat, err = currentStat(tx, userID); err != nil {
			return userStat, targetStat, err
		}
		if targetStat, err = currentStat(tx, targetUserID); err != nil {
			return userStat, targetStat, err
		}
		return userStat, targetStat, nil
	}
	var err error
	if targetStat, err = currentStat(tx, targetUserID); err != nil {
		return userStat, targetStat, err
	}
	if userStat, err = currentStat(tx, userID); err != nil {
		return userStat, targetStat, err
	}
	return userStat, targetStat, nil
}

func updateRelationStatsOrdered(tx *gorm.DB, userID int64, targetUserID int64, delta int) (RelationStatModel, RelationStatModel, error) {
	var userStat, targetStat RelationStatModel
	if userID <= targetUserID {
		var err error
		if userStat, err = updateStat(tx, userID, delta, 0); err != nil {
			return userStat, targetStat, err
		}
		if targetStat, err = updateStat(tx, targetUserID, 0, delta); err != nil {
			return userStat, targetStat, err
		}
		return userStat, targetStat, nil
	}
	var err error
	if targetStat, err = updateStat(tx, targetUserID, 0, delta); err != nil {
		return userStat, targetStat, err
	}
	if userStat, err = updateStat(tx, userID, delta, 0); err != nil {
		return userStat, targetStat, err
	}
	return userStat, targetStat, nil
}

func ensureStat(tx *gorm.DB, userID int64) error {
	stat := RelationStatModel{UserID: userID}
	return tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&stat).Error
}

func currentStat(tx *gorm.DB, userID int64) (RelationStatModel, error) {
	var stat RelationStatModel
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).
		Where("user_id = ?", userID).
		Take(&stat).
		Error
	return stat, err
}

func updateStat(tx *gorm.DB, userID int64, followingDelta int, followerDelta int) (RelationStatModel, error) {
	stat, err := currentStat(tx, userID)
	if err != nil {
		return stat, err
	}
	stat.FollowingCount = clampCount(stat.FollowingCount + followingDelta)
	stat.FollowerCount = clampCount(stat.FollowerCount + followerDelta)
	if err := tx.Save(&stat).Error; err != nil {
		return stat, err
	}
	return stat, nil
}

func restoreFollow(model FollowModel) *domainrelation.Follow {
	return domainrelation.RestoreFollow(
		model.ID,
		model.UserID,
		model.TargetUserID,
		model.Status,
		idempotencyKeyValue(model.IdempotencyKey),
		model.CreatedAt,
		model.UpdatedAt,
	)
}

func restoreStat(model RelationStatModel) *domainrelation.RelationStat {
	return domainrelation.RestoreRelationStat(
		model.UserID,
		model.FollowingCount,
		model.FollowerCount,
		model.CreatedAt,
		model.UpdatedAt,
	)
}

func idempotencyKeyPtr(value string) *string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	return &value
}

func idempotencyKeyValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func mapUserError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return domainrelation.ErrTargetUserNotFound
	}
	return err
}

func clampCount(value int) int {
	if value < 0 {
		return 0
	}
	return value
}

var _ domainrelation.Repository = (*Repository)(nil)
