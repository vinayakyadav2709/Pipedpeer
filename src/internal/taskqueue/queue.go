// Package taskqueue provides durable task submission and consumption
// using NATS JetStream. Tasks are published to a stream and consumed
// by workers with automatic retry, backoff, and dead-letter support.
package taskqueue

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nats.go/jetstream"

	"github.com/pipedpeer/pipedpeer/internal/logging"
	"github.com/pipedpeer/pipedpeer/internal/natsbus"
)

const (
	streamName    = "PIPEDPEER_TASKS"
	subjectPrefix = "pipedpeer.tasks"
)

// Task represents a submitted compute task.
type Task struct {
	ID               string `json:"id"`
	ScriptPath       string `json:"script_path"`
	SubmitterNode    string `json:"submitter_node"`
	RequiredMemBytes int64  `json:"required_mem_bytes,omitempty"`
	EstimationTier   string `json:"estimation_tier,omitempty"`
	JobName          string `json:"job_name,omitempty"`
	Status           string `json:"status"` // pending, running, succeeded, failed, dead
	Attempt          int    `json:"attempt"`
	CreatedAt        string `json:"created_at"`
	UpdatedAt        string `json:"updated_at"`
}

// Queue manages task submission and consumption via JetStream.
type Queue struct {
	bus    *natsbus.Bus
	stream jetstream.Stream
	log    func() interface{ Msg(string) }
}

// Config controls queue behavior.
type Config struct {
	// MaxDeliver is the max number of delivery attempts before dead-lettering.
	MaxDeliver int
	// AckWait is how long a consumer has to ack before redelivery.
	AckWait time.Duration
	// BackOff specifies retry intervals. If empty, uses exponential backoff.
	BackOff []time.Duration
}

// DefaultConfig returns sensible defaults for task queue behavior.
func DefaultConfig() Config {
	return Config{
		MaxDeliver: 5,
		AckWait:    30 * time.Second,
		BackOff: []time.Duration{
			1 * time.Second,
			5 * time.Second,
			15 * time.Second,
			60 * time.Second,
		},
	}
}

// New creates or connects to the task queue stream.
func New(bus *natsbus.Bus, cfg Config) (*Queue, error) {
	if cfg.MaxDeliver == 0 {
		cfg = DefaultConfig()
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create or update the stream
	streamCfg := jetstream.StreamConfig{
		Name:      streamName,
		Subjects:  []string{subjectPrefix + ".>"},
		Retention: jetstream.WorkQueuePolicy,
		MaxAge:    24 * time.Hour,
		Storage:   jetstream.FileStorage,
	}

	stream, err := bus.JS.CreateOrUpdateStream(ctx, streamCfg)
	if err != nil {
		return nil, fmt.Errorf("taskqueue: create stream: %w", err)
	}

	log := logging.WithComponent("taskqueue")
	log.Info().Str("stream", streamName).Msg("task queue initialized")

	return &Queue{bus: bus, stream: stream}, nil
}

// Submit publishes a task to the queue for processing.
func (q *Queue) Submit(task Task) error {
	task.Status = "pending"
	task.CreatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	task.UpdatedAt = task.CreatedAt
	task.Attempt = 0

	data, err := json.Marshal(task)
	if err != nil {
		return fmt.Errorf("taskqueue: marshal task: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = q.bus.JS.Publish(ctx, subjectPrefix+".submit", data)
	if err != nil {
		return fmt.Errorf("taskqueue: publish: %w", err)
	}

	log := logging.WithComponent("taskqueue")
	log.Info().Str("task_id", task.ID).Str("submitter", task.SubmitterNode).Msg("task submitted")
	return nil
}

// TaskHandler is called for each task delivered from the queue.
// Return nil to ack (task succeeded), return error to nack (will retry).
type TaskHandler func(task Task) error

// Consume creates a durable consumer and processes tasks.
// Blocks until ctx is cancelled.
func (q *Queue) Consume(ctx context.Context, workerName string, handler TaskHandler, cfg Config) error {
	if cfg.MaxDeliver == 0 {
		cfg = DefaultConfig()
	}

	consumerCfg := jetstream.ConsumerConfig{
		Durable:       workerName,
		AckWait:       cfg.AckWait,
		MaxDeliver:    cfg.MaxDeliver,
		FilterSubject: subjectPrefix + ".submit",
		BackOff:       cfg.BackOff,
	}

	consumer, err := q.stream.CreateOrUpdateConsumer(ctx, consumerCfg)
	if err != nil {
		return fmt.Errorf("taskqueue: create consumer: %w", err)
	}

	log := logging.WithComponent("taskqueue")
	log.Info().Str("consumer", workerName).Msg("consuming tasks")

	// Consume messages
	cc, err := consumer.Consume(func(msg jetstream.Msg) {
		var task Task
		if err := json.Unmarshal(msg.Data(), &task); err != nil {
			log.Error().Err(err).Msg("invalid task message")
			_ = msg.Term() // terminal error, don't retry
			return
		}

		meta, _ := msg.Metadata()
		if meta != nil {
			task.Attempt = int(meta.NumDelivered)
		}

		log.Info().
			Str("task_id", task.ID).
			Int("attempt", task.Attempt).
			Msg("processing task")

		if err := handler(task); err != nil {
			log.Warn().
				Err(err).
				Str("task_id", task.ID).
				Int("attempt", task.Attempt).
				Msg("task failed, will retry")
			_ = msg.Nak() // negative ack → redeliver with backoff
			return
		}

		_ = msg.Ack()
		log.Info().Str("task_id", task.ID).Msg("task completed")
	})
	if err != nil {
		return fmt.Errorf("taskqueue: consume: %w", err)
	}
	defer cc.Stop()

	// Block until context cancelled
	<-ctx.Done()
	return ctx.Err()
}

// StreamName returns the JetStream stream name.
func (q *Queue) StreamName() string {
	return streamName
}

// PendingCount returns the number of pending messages in the stream.
func (q *Queue) PendingCount(ctx context.Context) (uint64, error) {
	info, err := q.stream.Info(ctx)
	if err != nil {
		return 0, err
	}
	return info.State.Msgs, nil
}
