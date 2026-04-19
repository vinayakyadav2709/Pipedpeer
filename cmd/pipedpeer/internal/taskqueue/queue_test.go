package taskqueue

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/natsbus"
)

func TestSubmitAndConsume(t *testing.T) {
	bus, err := natsbus.New(natsbus.Config{Embedded: true, StoreDir: t.TempDir()})
	if err != nil {
		t.Fatalf("create bus: %v", err)
	}
	defer bus.Close()

	q, err := New(bus, DefaultConfig())
	if err != nil {
		t.Fatalf("create queue: %v", err)
	}

	// Submit a task
	task := Task{
		ID:            "test-task-1",
		ScriptPath:    "/tmp/train.py",
		SubmitterNode: "node-a",
		JobName:       "train-model",
	}
	if err := q.Submit(task); err != nil {
		t.Fatalf("submit: %v", err)
	}

	// Verify pending count
	ctx := context.Background()
	count, err := q.PendingCount(ctx)
	if err != nil {
		t.Fatalf("pending count: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 pending task, got %d", count)
	}

	// Consume the task
	var processed atomic.Int32
	var receivedTask Task

	consumeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	go func() {
		_ = q.Consume(consumeCtx, "test-worker", func(t Task) error {
			processed.Add(1)
			receivedTask = t
			cancel() // stop after first task
			return nil
		}, DefaultConfig())
	}()

	<-consumeCtx.Done()
	time.Sleep(100 * time.Millisecond) // let ack propagate

	if processed.Load() != 1 {
		t.Fatalf("expected 1 processed task, got %d", processed.Load())
	}
	if receivedTask.ID != "test-task-1" {
		t.Fatalf("expected task ID test-task-1, got %s", receivedTask.ID)
	}
	if receivedTask.SubmitterNode != "node-a" {
		t.Fatalf("expected submitter node-a, got %s", receivedTask.SubmitterNode)
	}
	if receivedTask.Status != "pending" {
		t.Fatalf("expected status pending, got %s", receivedTask.Status)
	}
}

func TestRetryOnFailure(t *testing.T) {
	bus, err := natsbus.New(natsbus.Config{Embedded: true, StoreDir: t.TempDir()})
	if err != nil {
		t.Fatalf("create bus: %v", err)
	}
	defer bus.Close()

	cfg := Config{
		MaxDeliver: 3,
		AckWait:    500 * time.Millisecond,
		BackOff:    []time.Duration{100 * time.Millisecond, 200 * time.Millisecond},
	}

	q, err := New(bus, cfg)
	if err != nil {
		t.Fatalf("create queue: %v", err)
	}

	if err := q.Submit(Task{ID: "retry-task", SubmitterNode: "node-b"}); err != nil {
		t.Fatalf("submit: %v", err)
	}

	var attempts atomic.Int32

	consumeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() {
		_ = q.Consume(consumeCtx, "retry-worker", func(task Task) error {
			n := attempts.Add(1)
			if n < 3 {
				return fmt.Errorf("simulated failure %d", n)
			}
			cancel() // succeed on 3rd attempt
			return nil
		}, cfg)
	}()

	<-consumeCtx.Done()
	time.Sleep(200 * time.Millisecond)

	finalAttempts := attempts.Load()
	if finalAttempts < 2 {
		t.Fatalf("expected at least 2 attempts (failures + success), got %d", finalAttempts)
	}
}

func TestMultipleTasksProcessedInOrder(t *testing.T) {
	bus, err := natsbus.New(natsbus.Config{Embedded: true, StoreDir: t.TempDir()})
	if err != nil {
		t.Fatalf("create bus: %v", err)
	}
	defer bus.Close()

	q, err := New(bus, DefaultConfig())
	if err != nil {
		t.Fatalf("create queue: %v", err)
	}

	// Submit 3 tasks
	for i := 1; i <= 3; i++ {
		if err := q.Submit(Task{
			ID:            fmt.Sprintf("task-%d", i),
			SubmitterNode: "node-c",
		}); err != nil {
			t.Fatalf("submit task-%d: %v", i, err)
		}
	}

	count, _ := q.PendingCount(context.Background())
	if count != 3 {
		t.Fatalf("expected 3 pending tasks, got %d", count)
	}

	// Consume all 3
	var order []string
	var processed atomic.Int32

	consumeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	go func() {
		_ = q.Consume(consumeCtx, "order-worker", func(task Task) error {
			order = append(order, task.ID)
			if processed.Add(1) >= 3 {
				cancel()
			}
			return nil
		}, DefaultConfig())
	}()

	<-consumeCtx.Done()
	time.Sleep(100 * time.Millisecond)

	if processed.Load() != 3 {
		t.Fatalf("expected 3 processed tasks, got %d", processed.Load())
	}
	// WorkQueue should deliver in submission order
	if len(order) >= 3 && order[0] != "task-1" {
		t.Fatalf("expected first task to be task-1, got %s", order[0])
	}
}
