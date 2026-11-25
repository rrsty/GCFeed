package main

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	domainembedding "GCFeed/internal/domain/embedding"
	domaininteraction "GCFeed/internal/domain/interaction"
	infraconfig "GCFeed/internal/infra/config"
	infraaccount "GCFeed/internal/infra/persistence/account"
	infraembedding "GCFeed/internal/infra/persistence/embedding"
	infraexposure "GCFeed/internal/infra/persistence/exposure"
	infrafeed "GCFeed/internal/infra/persistence/feed"
	infrainteraction "GCFeed/internal/infra/persistence/interaction"
	migration "GCFeed/internal/infra/persistence/migration"
	infrarelation "GCFeed/internal/infra/persistence/relation"
	infravideo "GCFeed/internal/infra/persistence/video"

	_ "github.com/go-sql-driver/mysql"
	"golang.org/x/crypto/bcrypt"
	gormmysql "gorm.io/driver/mysql"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

var (
	configPath = flag.String("config", "./configs/config.yaml", "配置文件路径")
	reset      = flag.Bool("reset", false, "是否清空已有数据重新生成")
	userCount  = flag.Int("users", 20, "生成的测试用户数量")
	videoCount = flag.Int("videos", 100, "生成的测试视频数量")
)

var sampleTitles = []string{
	"Golang 高并发 Feed 流架构实战解析",
	"Redis 亿级热榜与二级缓存设计",
	"4K 治愈系：大自然森林晨光与鸟鸣",
	"咖啡大师手把手教你拉心形花纹",
	"街头漫步：东京秋叶原雨夜霓虹",
	"10分钟学会做一份完美的意式肉酱面",
	"可爱猫咪的迷惑行为大赏",
	"沉浸式赛博朋克键盘敲击音 ASMR",
	"航拍中国：雪山与湖泊的壮丽画卷",
	"前端性能优化：从 3 秒到 0.3 秒",
	"吉他弹唱《晴天》指弹前奏教学",
	"居家高效哑铃全身力量训练指南",
	"微服务架构下消息队列解耦的最佳实践",
	"夜市烟火气：寻找城市最好吃的小吃摊",
	"机械键盘轴体手感对比与选购指南",
}

var sampleCovers = []string{
	"https://images.unsplash.com/photo-1517694712202-14dd9538aa97",
	"https://images.unsplash.com/photo-1506744038136-46273834b3fb",
	"https://images.unsplash.com/photo-1507525428034-b723cf961d3e",
	"https://images.unsplash.com/photo-1514432324607-a09d9b4aefdd",
	"https://images.unsplash.com/photo-1514888286974-6c03e2ca1dba",
	"https://images.unsplash.com/photo-1498050108023-c5249f4df085",
	"https://images.unsplash.com/photo-1511671782779-c97d3d27a1d4",
	"https://images.unsplash.com/photo-1534438327276-14e5300c3a48",
	"https://images.unsplash.com/photo-1518770660439-4636190af475",
	"https://images.unsplash.com/photo-1542291026-7eec264c27ff",
}

const sampleVideoURL = "https://interactive-examples.mdn.mozilla.net/media/cc0-videos/flower.mp4"

func main() {
	flag.Parse()
	if *userCount <= 0 {
		log.Fatalf("users 参数必须大于 0: %d", *userCount)
	}
	if *videoCount < 0 {
		log.Fatalf("videos 参数不能为负数: %d", *videoCount)
	}

	// 自动适配工作目录寻找配置
	cfgFile := *configPath
	if _, err := os.Stat(cfgFile); os.IsNotExist(err) {
		// 尝试在 apps/api 目录下寻找
		altPath := filepath.Join("apps", "api", cfgFile)
		if _, err := os.Stat(altPath); err == nil {
			cfgFile = altPath
		}
	}

	cfg, err := infraconfig.LoadConfig(cfgFile)
	if err != nil {
		log.Fatalf("加载配置失败 (%s): %v", cfgFile, err)
	}

	log.Printf("准备连接数据库: host=%s port=%d user=%s db=%s",
		cfg.Database.Host, cfg.Database.Port, cfg.Database.User, cfg.Database.Name)

	// 1. 确保目标数据库存在（增加重试机制，适应 Docker Compose 冷启动）
	rootDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8mb4&parseTime=true&loc=Local",
		cfg.Database.User, cfg.Database.Password, cfg.Database.Host, cfg.Database.Port)
	var rawDB *sql.DB
	for attempt := 1; attempt <= 15; attempt++ {
		rawDB, err = sql.Open("mysql", rootDSN)
		if err == nil {
			if pingErr := rawDB.Ping(); pingErr == nil {
				break
			} else {
				_ = rawDB.Close()
				rawDB = nil
				err = pingErr
			}
		}
		log.Printf("等待 MySQL 就绪 (重试 %d/15): %v", attempt, err)
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		log.Fatalf("连接 MySQL 最终失败: %v", err)
	}
	defer rawDB.Close()

	createDBSQL := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s` DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;", cfg.Database.Name)
	if _, err := rawDB.Exec(createDBSQL); err != nil {
		log.Fatalf("创建数据库 %s 失败: %v", cfg.Database.Name, err)
	}
	log.Printf("数据库 [%s] 检查就绪", cfg.Database.Name)

	// 2. 连接业务数据库并通过 GORM AutoMigrate 建表
	appDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8mb4&parseTime=true&loc=Local",
		cfg.Database.User, cfg.Database.Password, cfg.Database.Host, cfg.Database.Port, cfg.Database.Name)
	var gormDB *gorm.DB
	for attempt := 1; attempt <= 5; attempt++ {
		gormDB, err = gorm.Open(gormmysql.Open(appDSN), &gorm.Config{
			Logger: logger.Default.LogMode(logger.Warn),
		})
		if err == nil {
			break
		}
		time.Sleep(1 * time.Second)
	}
	if err != nil {
		log.Fatalf("连接 GORM 失败: %v", err)
	}
	if sqlConn, dbErr := gormDB.DB(); dbErr == nil {
		defer sqlConn.Close()
	}

	log.Println("执行 AutoMigrate 检查与初始化数据表...")
	if err := migration.AutoMigrate(gormDB); err != nil {
		log.Fatalf("AutoMigrate 失败: %v", err)
	}
	log.Println("数据表结构初始化成功！")

	// 如果需要 reset，清空数据（使用各模型真实表名并检查错误）
	if *reset {
		log.Println("清理旧数据...")
		tables := []string{
			"account",
			"video",
			"video_stat",
			"video_embedding",
			"user_follow",
			"user_relation_stat",
			"interaction_action",
			"interaction_comment",
			"feed_inbox",
			"video_view_events",
			"exposures",
			"user_message",
			"playback_config",
			"playback_qos_log",
		}
		for _, table := range tables {
			if err := gormDB.Exec("TRUNCATE TABLE `" + table + "`").Error; err != nil {
				log.Printf("清理表 %s 警告: %v", table, err)
			}
		}
	}

	// 3. 生成测试用户
	hashedPassword, err := bcrypt.GenerateFromPassword([]byte("123456"), bcrypt.DefaultCost)
	if err != nil {
		log.Fatalf("生成密码哈希失败: %v", err)
	}

	users := make([]infraaccount.UserModel, 0, *userCount)
	for i := 1; i <= *userCount; i++ {
		account := fmt.Sprintf("test_user_%d", i)
		nickname := fmt.Sprintf("创作者%02d号", i)
		avatar := sampleCovers[(i-1)%len(sampleCovers)] + "?auto=format&fit=crop&w=200&h=200&q=80"
		users = append(users, infraaccount.UserModel{
			ID:        int64(i),
			Account:   account,
			Password:  string(hashedPassword),
			Nickname:  nickname,
			AvatarURL: avatar,
			Bio:       fmt.Sprintf("这是 %s 的主页，热爱技术与生活分享", nickname),
			Status:    1,
			Role:      "user",
		})
	}

	if err := gormDB.Clauses(clause.OnConflict{DoNothing: true}).Create(&users).Error; err != nil {
		log.Fatalf("插入用户数据失败: %v", err)
	}
	log.Printf("测试用户注入完成: 数量=%d (默认账号 test_user_1 ~ test_user_%d，统一密码 123456)", len(users), len(users))

	// 4. 生成测试视频及统计数据
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	videos := make([]infravideo.VideoModel, 0, *videoCount)
	stats := make([]infravideo.VideoStatModel, 0, *videoCount)
	now := time.Now()

	for i := 1; i <= *videoCount; i++ {
		authorID := int64((i-1)%len(users) + 1)
		titleIdx := (i - 1) % len(sampleTitles)
		coverIdx := (i - 1) % len(sampleCovers)

		// 错开发布时间（每条视频相隔 10~30 分钟），方便分页排序测试
		publishTime := now.Add(-time.Duration((*videoCount-i)*15) * time.Minute)

		title := fmt.Sprintf("%s #%d", sampleTitles[titleIdx], i)
		cover := fmt.Sprintf("%s?auto=format&fit=crop&w=800&q=80", sampleCovers[coverIdx])

		videos = append(videos, infravideo.VideoModel{
			ID:          int64(i),
			AuthorID:    authorID,
			Title:       title,
			Description: fmt.Sprintf("关于 %s 的详细内容介绍，欢迎点赞和关注！", title),
			MediaURL:    sampleVideoURL,
			CoverURL:    cover,
			Status:      2, // 2 = 已发布 (StatusPublished)
			PublishedAt: &publishTime,
			CreatedAt:   publishTime,
			UpdatedAt:   publishTime,
		})

		// 生成热度梯度：部分高热度视频（用于热门榜压测），部分普通视频
		likeBase := r.Intn(500)
		if i%5 == 0 {
			likeBase = 1000 + r.Intn(9000) // 热门视频
		}

		stats = append(stats, infravideo.VideoStatModel{
			VideoID:       int64(i),
			LikeCount:     likeBase,
			CommentCount:  likeBase / (r.Intn(5) + 3),
			FavoriteCount: likeBase / (r.Intn(4) + 2),
		})
	}

	if err := gormDB.Clauses(clause.OnConflict{DoNothing: true}).Create(&videos).Error; err != nil {
		log.Fatalf("插入视频数据失败: %v", err)
	}
	// 使用 DoNothing 避免 Docker Compose 重启时用随机值覆盖线上已有互动计数。
	if err := gormDB.Clauses(clause.OnConflict{DoNothing: true}).Create(&stats).Error; err != nil {
		log.Fatalf("插入视频统计数据失败: %v", err)
	}
	log.Printf("测试视频注入完成: 视频数=%d, 统计数据数=%d", len(videos), len(stats))

	// 生成视频内容特征向量（用于推荐流协同过滤与向量相似度计算）
	vectorizer := domainembedding.NewHashNgramVectorizer()
	embeddings := make([]infraembedding.VideoEmbeddingModel, 0, len(videos))
	for _, v := range videos {
		vec := vectorizer.Vectorize(v.Title + " " + v.Description)
		vecJSON, _ := json.Marshal(vec)
		h := sha256.Sum256([]byte(v.Title + " " + v.Description))
		embeddings = append(embeddings, infraembedding.VideoEmbeddingModel{
			VideoID:       v.ID,
			Model:         vectorizer.Model(),
			Dimension:     vectorizer.Dimension(),
			EmbeddingJSON: string(vecJSON),
			TextHash:      hex.EncodeToString(h[:]),
		})
	}
	if len(embeddings) > 0 {
		if err := gormDB.Clauses(clause.OnConflict{DoNothing: true}).Create(&embeddings).Error; err != nil {
			log.Printf("插入视频向量数据警告: %v", err)
		} else {
			log.Printf("视频向量注入完成: 向量数=%d", len(embeddings))
		}
	}

	// 5. 生成部分关注关系和真实一致的关系统计
	follows := make([]infrarelation.FollowModel, 0)
	followingCountMap := make(map[int64]int, len(users))
	followerCountMap := make(map[int64]int, len(users))
	for _, u := range users {
		for _, target := range users {
			if u.ID != target.ID && (u.ID+target.ID)%3 == 0 {
				follows = append(follows, infrarelation.FollowModel{
					UserID:       u.ID,
					TargetUserID: target.ID,
					Status:       1,
				})
				followingCountMap[u.ID]++
				followerCountMap[target.ID]++
			}
		}
	}
	relationStats := make([]infrarelation.RelationStatModel, 0, len(users))
	for _, u := range users {
		relationStats = append(relationStats, infrarelation.RelationStatModel{
			UserID:         u.ID,
			FollowingCount: followingCountMap[u.ID],
			FollowerCount:  followerCountMap[u.ID],
		})
	}

	if len(follows) > 0 {
		if err := gormDB.Clauses(clause.OnConflict{DoNothing: true}).Create(&follows).Error; err != nil {
			log.Printf("插入关注记录警告: %v", err)
		}
	}
	if err := gormDB.Clauses(clause.OnConflict{DoNothing: true}).Create(&relationStats).Error; err != nil {
		log.Printf("插入关系统计警告: %v", err)
	}
	log.Printf("关注与关系统计注入完成: 关注记录数=%d", len(follows))

	// 6. 为前 5 个用户对部分视频增加点赞行为记录（使用标准大写 ActionTypeLike，确保 Feed 红心状态正常展示）
	actions := make([]infrainteraction.ActionModel, 0)
	maxSeedUser := int64(min(5, len(users)))
	maxSeedVideo := int64(min(20, len(videos)))
	for uid := int64(1); uid <= maxSeedUser; uid++ {
		for vid := int64(1); vid <= maxSeedVideo; vid++ {
			if (uid+vid)%2 == 0 {
				actions = append(actions, infrainteraction.ActionModel{
					UserID:     uid,
					VideoID:    vid,
					ActionType: domaininteraction.ActionTypeLike,
					Status:     domaininteraction.ActionStatusActive,
				})
			}
		}
	}
	if len(actions) > 0 {
		if err := gormDB.Clauses(clause.OnConflict{DoNothing: true}).Create(&actions).Error; err != nil {
			log.Printf("插入互动点赞状态警告: %v", err)
		} else {
			log.Printf("互动点赞状态注入完成: 点赞记录数=%d", len(actions))
		}
	}

	// 7. 汇总状态
	var totalUsers, totalVideos, totalStats int64
	gormDB.Model(&infraaccount.UserModel{}).Count(&totalUsers)
	gormDB.Model(&infravideo.VideoModel{}).Count(&totalVideos)
	gormDB.Model(&infravideo.VideoStatModel{}).Count(&totalStats)

	// 消除未使用警告的 dummy
	_ = &infrafeed.InboxModel{}
	_ = &infraexposure.ViewEventModel{}

	fmt.Println("\n================ 数据库测试数据就绪 ================")
	fmt.Printf("当前数据库用户总数: %d\n", totalUsers)
	fmt.Printf("当前数据库视频总数: %d (已发布审核)\n", totalVideos)
	fmt.Printf("当前视频统计表总数: %d\n", totalStats)
	fmt.Println("测试账号示例: test_user_1  密码: 123456")
	fmt.Println("测试账号示例: test_user_2  密码: 123456")
	fmt.Println("====================================================")
}
