package inframq

import (
	applicationexposure "GCFeed/internal/application/exposure"
	applicationinteraction "GCFeed/internal/application/interaction"
	applicationvideo "GCFeed/internal/application/video"
	infraconfig "GCFeed/internal/infra/config"
	inframetrics "GCFeed/internal/infra/metrics"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const (
	defaultInteractionExchange     = "gcfeed.interaction"
	defaultActionChangedQueue      = "gcfeed.interaction.action_changed"
	defaultActionChangedRouting    = "interaction.action_changed"
	defaultVideoExchange           = "gcfeed.video"
	defaultVideoPublishedQueue     = "gcfeed.video.published"
	defaultVideoEmbeddingQueue     = "gcfeed.video.embedding"
	defaultVideoPublishedRouting   = "video.published"
	defaultExposureExchange        = "gcfeed.exposure"
	defaultViewEventRecordedQueue  = "gcfeed.exposure.view_event_recorded"
	defaultViewEventRecordedRoute  = "exposure.view_event_recorded"
	defaultDeadLetterExchange      = "gcfeed.dlx"
	defaultDeadLetterQueue         = "gcfeed.dlq"
	maxConsumeRetries              = 3
	defaultConsumerPrefetch        = 20
	publishConfirmTimeout          = 5 * time.Second
	headerRetryCount               = "x-retry-count"
	headerOriginalQueue            = "x-original-queue"
	headerErrorReason              = "x-error-reason"
)

var ErrEmptyRabbitMQURL = errors.New("rabbitmq url is empty")
var ErrPublishNotConfirmed = errors.New("rabbitmq publish was not confirmed")

type permanentMarker interface {
	Permanent() bool
}

type RabbitMQ struct {
	config infraconfig.RabbitMQConfig

	connMu         sync.RWMutex
	conn           *amqp.Connection
	pubMu          sync.Mutex
	publishChannel *amqp.Channel

	closedMu sync.Mutex
	closed   bool
}

func NewRabbitMQ(cfg infraconfig.RabbitMQConfig) (*RabbitMQ, error) {
	cfg = normalizeRabbitMQConfig(cfg)
	if strings.TrimSpace(cfg.URL) == "" {
		return nil, ErrEmptyRabbitMQURL
	}

	client := &RabbitMQ{config: cfg}
	if err := client.connect(); err != nil {
		return nil, err
	}
	return client, nil
}

func (r *RabbitMQ) connect() error {
	conn, err := amqp.Dial(r.config.URL)
	if err != nil {
		return err
	}
	pubCh, err := openPublishChannel(conn)
	if err != nil {
		_ = conn.Close()
		return err
	}
	if err := ensureTopology(pubCh, r.config); err != nil {
		_ = pubCh.Close()
		_ = conn.Close()
		return err
	}

	r.connMu.Lock()
	oldConn := r.conn
	r.conn = conn
	r.connMu.Unlock()

	r.pubMu.Lock()
	oldPub := r.publishChannel
	r.publishChannel = pubCh
	r.pubMu.Unlock()

	if oldPub != nil {
		_ = oldPub.Close()
	}
	if oldConn != nil {
		_ = oldConn.Close()
	}
	return nil
}

func openPublishChannel(conn *amqp.Connection) (*amqp.Channel, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	// 开启 Publisher Confirm，保证消息真正被 broker 接收后才返回成功。
	if err := ch.Confirm(false); err != nil {
		_ = ch.Close()
		return nil, err
	}
	return ch, nil
}

func (r *RabbitMQ) Close() error {
	if r == nil {
		return nil
	}
	r.closedMu.Lock()
	r.closed = true
	r.closedMu.Unlock()

	r.pubMu.Lock()
	pubCh := r.publishChannel
	r.publishChannel = nil
	r.pubMu.Unlock()
	if pubCh != nil {
		_ = pubCh.Close()
	}

	r.connMu.Lock()
	conn := r.conn
	r.conn = nil
	r.connMu.Unlock()
	if conn != nil {
		return conn.Close()
	}
	return nil
}

func (r *RabbitMQ) isClosed() bool {
	r.closedMu.Lock()
	defer r.closedMu.Unlock()
	return r.closed
}

// publishJSON 串行保护发布 Channel，并支持断线自动重建与 Publisher Confirm 校验。
func (r *RabbitMQ) publishJSON(ctx context.Context, exchange string, routingKey string, messageID string, body []byte, headers amqp.Table) error {
	r.pubMu.Lock()
	defer r.pubMu.Unlock()

	if err := r.ensurePublishChannelLocked(); err != nil {
		return err
	}

	confirmCtx := ctx
	var cancel context.CancelFunc
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		confirmCtx, cancel = context.WithTimeout(ctx, publishConfirmTimeout)
		defer cancel()
	}

	confirmation, err := r.publishChannel.PublishWithDeferredConfirmWithContext(
		confirmCtx,
		exchange,
		routingKey,
		false,
		false,
		amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			MessageId:    messageID,
			Timestamp:    time.Now(),
			Headers:      headers,
			Body:         body,
		},
	)
	if err != nil {
		return err
	}
	if confirmation != nil {
		acked, err := confirmation.WaitContext(confirmCtx)
		if err != nil {
			return err
		}
		if !acked {
			return ErrPublishNotConfirmed
		}
	}
	return nil
}

func (r *RabbitMQ) ensurePublishChannelLocked() error {
	if r.publishChannel != nil && !r.publishChannel.IsClosed() {
		return nil
	}

	r.connMu.RLock()
	conn := r.conn
	r.connMu.RUnlock()

	if conn == nil || conn.IsClosed() {
		newConn, err := amqp.Dial(r.config.URL)
		if err != nil {
			return err
		}
		pubCh, err := openPublishChannel(newConn)
		if err != nil {
			_ = newConn.Close()
			return err
		}
		if err := ensureTopology(pubCh, r.config); err != nil {
			_ = pubCh.Close()
			_ = newConn.Close()
			return err
		}
		r.connMu.Lock()
		r.conn = newConn
		r.connMu.Unlock()
		r.publishChannel = pubCh
		return nil
	}

	pubCh, err := openPublishChannel(conn)
	if err != nil {
		return err
	}
	r.publishChannel = pubCh
	return nil
}

func (r *RabbitMQ) openConsumerChannel() (*amqp.Channel, error) {
	r.connMu.RLock()
	conn := r.conn
	r.connMu.RUnlock()

	if conn == nil || conn.IsClosed() {
		if err := r.connect(); err != nil {
			return nil, err
		}
		r.connMu.RLock()
		conn = r.conn
		r.connMu.RUnlock()
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	// 每个消费者独占一个 Channel 和独立 QoS(20)，避免多协程共享 Channel 引发 AMQP 帧交错。
	if err := ch.Qos(defaultConsumerPrefetch, 0, false); err != nil {
		_ = ch.Close()
		return nil, err
	}
	return ch, nil
}

func (r *RabbitMQ) PublishActionChanged(ctx context.Context, event *applicationinteraction.ActionChangedEvent) error {
	if event == nil {
		return nil
	}
	content, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return r.publishJSON(ctx, r.config.InteractionExchange, r.config.ActionChangedRouting, event.EventID, content, nil)
}

func (r *RabbitMQ) PublishVideoPublished(ctx context.Context, event *applicationvideo.PublishedEvent) error {
	if event == nil {
		return nil
	}
	content, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return r.publishJSON(ctx, r.config.VideoExchange, r.config.VideoPublishedRouting, event.EventID, content, nil)
}

func (r *RabbitMQ) PublishViewEventRecorded(ctx context.Context, event *applicationexposure.ViewEventRecordedEvent) error {
	if event == nil {
		return nil
	}
	content, err := json.Marshal(event)
	if err != nil {
		return err
	}
	return r.publishJSON(ctx, r.config.ExposureExchange, r.config.ViewEventRecordedRouting, event.EventID, content, nil)
}

func (r *RabbitMQ) ConsumeActionChanged(ctx context.Context, handler func(context.Context, *applicationinteraction.ActionChangedEvent) error) error {
	return r.startConsumer(
		ctx,
		r.config.ActionChangedQueue,
		r.config.InteractionExchange,
		r.config.ActionChangedRouting,
		"mq_action_changed",
		func(c context.Context, body []byte) error {
			var event applicationinteraction.ActionChangedEvent
			if err := json.Unmarshal(body, &event); err != nil {
				return decodeError{err: err}
			}
			return handler(c, &event)
		},
	)
}

func (r *RabbitMQ) ConsumeVideoPublished(ctx context.Context, handler func(context.Context, *applicationvideo.PublishedEvent) error) error {
	return r.consumeVideoPublishedQueue(ctx, r.config.VideoPublishedQueue, handler)
}

func (r *RabbitMQ) ConsumeVideoPublishedForEmbedding(ctx context.Context, handler func(context.Context, *applicationvideo.PublishedEvent) error) error {
	return r.consumeVideoPublishedQueue(ctx, r.config.VideoEmbeddingQueue, handler)
}

func (r *RabbitMQ) consumeVideoPublishedQueue(ctx context.Context, queue string, handler func(context.Context, *applicationvideo.PublishedEvent) error) error {
	return r.startConsumer(
		ctx,
		queue,
		r.config.VideoExchange,
		r.config.VideoPublishedRouting,
		"mq_video_published",
		func(c context.Context, body []byte) error {
			var event applicationvideo.PublishedEvent
			if err := json.Unmarshal(body, &event); err != nil {
				return decodeError{err: err}
			}
			return handler(c, &event)
		},
	)
}

func (r *RabbitMQ) ConsumeViewEventRecorded(ctx context.Context, handler func(context.Context, *applicationexposure.ViewEventRecordedEvent) error) error {
	return r.startConsumer(
		ctx,
		r.config.ViewEventRecordedQueue,
		r.config.ExposureExchange,
		r.config.ViewEventRecordedRouting,
		"mq_view_event",
		func(c context.Context, body []byte) error {
			var event applicationexposure.ViewEventRecordedEvent
			if err := json.Unmarshal(body, &event); err != nil {
				return decodeError{err: err}
			}
			return handler(c, &event)
		},
	)
}

type decodeError struct {
	err error
}

func (e decodeError) Error() string   { return e.err.Error() }
func (e decodeError) Unwrap() error   { return e.err }
func (e decodeError) Permanent() bool { return true }

// startConsumer 为单个队列分配独立 Channel，并提供断线指数退避重连、有限次重试与死信队列隔离。
func (r *RabbitMQ) startConsumer(
	ctx context.Context,
	queue string,
	retryExchange string,
	retryRoutingKey string,
	metricPrefix string,
	process func(context.Context, []byte) error,
) error {
	ch, deliveries, err := r.subscribeQueue(ctx, queue)
	if err != nil {
		return err
	}

	go func() {
		backoff := time.Second
		for {
			r.drainDeliveries(ctx, queue, retryExchange, retryRoutingKey, metricPrefix, deliveries, process)
			if ch != nil {
				_ = ch.Close()
				ch = nil
			}
			if ctx.Err() != nil || r.isClosed() {
				return
			}

			log.Printf("rabbitmq consumer disconnected (queue=%s), reconnecting in %s", queue, backoff)
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}

			nextCh, nextDeliveries, subErr := r.subscribeQueue(ctx, queue)
			if subErr != nil {
				log.Printf("rabbitmq consumer reconnect failed (queue=%s): %v", queue, subErr)
				backoff = min(backoff*2, 10*time.Second)
				continue
			}
			ch = nextCh
			deliveries = nextDeliveries
			backoff = time.Second
			log.Printf("rabbitmq consumer reconnected (queue=%s)", queue)
		}
	}()
	return nil
}

func (r *RabbitMQ) subscribeQueue(ctx context.Context, queue string) (*amqp.Channel, <-chan amqp.Delivery, error) {
	ch, err := r.openConsumerChannel()
	if err != nil {
		return nil, nil, err
	}
	deliveries, err := ch.ConsumeWithContext(ctx, queue, "", false, false, false, false, nil)
	if err != nil {
		_ = ch.Close()
		return nil, nil, err
	}
	return ch, deliveries, nil
}

func (r *RabbitMQ) drainDeliveries(
	ctx context.Context,
	queue string,
	retryExchange string,
	retryRoutingKey string,
	metricPrefix string,
	deliveries <-chan amqp.Delivery,
	process func(context.Context, []byte) error,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case delivery, ok := <-deliveries:
			if !ok {
				return
			}
			r.handleDelivery(ctx, queue, retryExchange, retryRoutingKey, metricPrefix, delivery, process)
		}
	}
}

func (r *RabbitMQ) handleDelivery(
	ctx context.Context,
	queue string,
	retryExchange string,
	retryRoutingKey string,
	metricPrefix string,
	delivery amqp.Delivery,
	process func(context.Context, []byte) error,
) {
	start := time.Now()
	err := process(ctx, delivery.Body)
	if err == nil {
		inframetrics.ObserveWorkerJob(metricPrefix+"_consume", time.Since(start), nil)
		_ = delivery.Ack(false)
		return
	}

	var decErr decodeError
	if errors.As(err, &decErr) {
		inframetrics.ObserveWorkerJob(metricPrefix+"_decode", time.Since(start), err)
	} else {
		inframetrics.ObserveWorkerJob(metricPrefix+"_consume", time.Since(start), err)
	}

	// 永久性业务错误（如视频已删除、消息格式损坏）直接投递死信队列，避免毒丸无限重试阻塞队列。
	if isPermanentError(err) {
		r.routeToDLQ(ctx, queue, delivery, retryCountFromHeaders(delivery.Headers), err)
		_ = delivery.Ack(false)
		return
	}

	retries := retryCountFromHeaders(delivery.Headers)
	if retries >= maxConsumeRetries {
		r.routeToDLQ(ctx, queue, delivery, retries, err)
		_ = delivery.Ack(false)
		return
	}

	// 有限次退避重试：通过 x-retry-count 重投原队列，重投失败时回退 Nack(requeue=true) 防止丢消息。
	wait := time.Duration(retries+1) * 100 * time.Millisecond
	select {
	case <-ctx.Done():
		_ = delivery.Nack(false, true)
		return
	case <-time.After(wait):
	}

	headers := cloneHeaders(delivery.Headers)
	headers[headerRetryCount] = int32(retries + 1)
	// 通过默认直连交换机 ("") 精准重投回当前 queue，避免 topic 广播到同 routingKey 的其他队列（如 fanout 与 embedding 互串）。
	if pubErr := r.publishJSON(ctx, "", queue, delivery.MessageId, delivery.Body, headers); pubErr != nil {
		log.Printf("rabbitmq retry republish failed (queue=%s): %v", queue, pubErr)
		_ = delivery.Nack(false, true)
		return
	}
	_ = delivery.Ack(false)
}

func (r *RabbitMQ) routeToDLQ(ctx context.Context, queue string, delivery amqp.Delivery, retries int, cause error) {
	headers := cloneHeaders(delivery.Headers)
	headers[headerOriginalQueue] = queue
	headers[headerRetryCount] = int32(retries)
	if cause != nil {
		headers[headerErrorReason] = cause.Error()
	}
	dlqCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 3*time.Second)
	defer cancel()
	if err := r.publishJSON(dlqCtx, defaultDeadLetterExchange, "dlq."+queue, delivery.MessageId, delivery.Body, headers); err != nil {
		log.Printf("rabbitmq publish to dlq failed (queue=%s): %v", queue, err)
	}
}

func isPermanentError(err error) bool {
	var marker permanentMarker
	return errors.As(err, &marker) && marker.Permanent()
}

func retryCountFromHeaders(headers amqp.Table) int {
	if headers == nil {
		return 0
	}
	raw, ok := headers[headerRetryCount]
	if !ok {
		return 0
	}
	switch v := raw.(type) {
	case int:
		return v
	case int32:
		return int(v)
	case int64:
		return int(v)
	case float64:
		return int(v)
	default:
		var parsed int
		_, _ = fmt.Sscan(fmt.Sprint(v), &parsed)
		return parsed
	}
}

func cloneHeaders(src amqp.Table) amqp.Table {
	dst := amqp.Table{}
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func ensureTopology(ch *amqp.Channel, cfg infraconfig.RabbitMQConfig) error {
	if err := ch.ExchangeDeclare(cfg.InteractionExchange, "topic", true, false, false, false, nil); err != nil {
		return err
	}
	if _, err := ch.QueueDeclare(cfg.ActionChangedQueue, true, false, false, false, nil); err != nil {
		return err
	}
	if err := ch.QueueBind(cfg.ActionChangedQueue, cfg.ActionChangedRouting, cfg.InteractionExchange, false, nil); err != nil {
		return err
	}

	if err := ch.ExchangeDeclare(cfg.VideoExchange, "topic", true, false, false, false, nil); err != nil {
		return err
	}
	if _, err := ch.QueueDeclare(cfg.VideoPublishedQueue, true, false, false, false, nil); err != nil {
		return err
	}
	if err := ch.QueueBind(cfg.VideoPublishedQueue, cfg.VideoPublishedRouting, cfg.VideoExchange, false, nil); err != nil {
		return err
	}
	if _, err := ch.QueueDeclare(cfg.VideoEmbeddingQueue, true, false, false, false, nil); err != nil {
		return err
	}
	if err := ch.QueueBind(cfg.VideoEmbeddingQueue, cfg.VideoPublishedRouting, cfg.VideoExchange, false, nil); err != nil {
		return err
	}

	if err := ch.ExchangeDeclare(cfg.ExposureExchange, "topic", true, false, false, false, nil); err != nil {
		return err
	}
	if _, err := ch.QueueDeclare(cfg.ViewEventRecordedQueue, true, false, false, false, nil); err != nil {
		return err
	}
	if err := ch.QueueBind(cfg.ViewEventRecordedQueue, cfg.ViewEventRecordedRouting, cfg.ExposureExchange, false, nil); err != nil {
		return err
	}

	// 声明独立死信交换机与死信队列，接收永久性错误和超过重试上限的异常消息。
	if err := ch.ExchangeDeclare(defaultDeadLetterExchange, "topic", true, false, false, false, nil); err != nil {
		return err
	}
	if _, err := ch.QueueDeclare(defaultDeadLetterQueue, true, false, false, false, nil); err != nil {
		return err
	}
	return ch.QueueBind(defaultDeadLetterQueue, "#", defaultDeadLetterExchange, false, nil)
}

func normalizeRabbitMQConfig(cfg infraconfig.RabbitMQConfig) infraconfig.RabbitMQConfig {
	cfg.URL = strings.TrimSpace(cfg.URL)
	cfg.InteractionExchange = strings.TrimSpace(cfg.InteractionExchange)
	cfg.ActionChangedQueue = strings.TrimSpace(cfg.ActionChangedQueue)
	cfg.ActionChangedRouting = strings.TrimSpace(cfg.ActionChangedRouting)
	if cfg.InteractionExchange == "" {
		cfg.InteractionExchange = defaultInteractionExchange
	}
	if cfg.ActionChangedQueue == "" {
		cfg.ActionChangedQueue = defaultActionChangedQueue
	}
	if cfg.ActionChangedRouting == "" {
		cfg.ActionChangedRouting = defaultActionChangedRouting
	}
	cfg.VideoExchange = strings.TrimSpace(cfg.VideoExchange)
	cfg.VideoPublishedQueue = strings.TrimSpace(cfg.VideoPublishedQueue)
	cfg.VideoEmbeddingQueue = strings.TrimSpace(cfg.VideoEmbeddingQueue)
	cfg.VideoPublishedRouting = strings.TrimSpace(cfg.VideoPublishedRouting)
	if cfg.VideoExchange == "" {
		cfg.VideoExchange = defaultVideoExchange
	}
	if cfg.VideoPublishedQueue == "" {
		cfg.VideoPublishedQueue = defaultVideoPublishedQueue
	}
	if cfg.VideoEmbeddingQueue == "" {
		cfg.VideoEmbeddingQueue = defaultVideoEmbeddingQueue
	}
	if cfg.VideoPublishedRouting == "" {
		cfg.VideoPublishedRouting = defaultVideoPublishedRouting
	}
	cfg.ExposureExchange = strings.TrimSpace(cfg.ExposureExchange)
	cfg.ViewEventRecordedQueue = strings.TrimSpace(cfg.ViewEventRecordedQueue)
	cfg.ViewEventRecordedRouting = strings.TrimSpace(cfg.ViewEventRecordedRouting)
	if cfg.ExposureExchange == "" {
		cfg.ExposureExchange = defaultExposureExchange
	}
	if cfg.ViewEventRecordedQueue == "" {
		cfg.ViewEventRecordedQueue = defaultViewEventRecordedQueue
	}
	if cfg.ViewEventRecordedRouting == "" {
		cfg.ViewEventRecordedRouting = defaultViewEventRecordedRoute
	}
	return cfg
}
