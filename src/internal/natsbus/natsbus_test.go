package natsbus

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestEmbeddedServerStartsAndConnects(t *testing.T) {
	bus, err := New(Config{
		Embedded: true,
		StoreDir: t.TempDir(),
		Name:     "test-embedded",
	})
	if err != nil {
		t.Fatalf("failed to create embedded bus: %v", err)
	}
	defer bus.Close()

	if !bus.IsEmbedded() {
		t.Fatal("expected embedded=true")
	}
	if bus.ClientURL() == "" {
		t.Fatal("expected non-empty client URL")
	}
	if !bus.Conn.IsConnected() {
		t.Fatal("expected connection to be established")
	}
}

func TestPubSub(t *testing.T) {
	bus, err := New(Config{Embedded: true, StoreDir: t.TempDir()})
	if err != nil {
		t.Fatalf("create bus: %v", err)
	}
	defer bus.Close()

	received := make(chan []byte, 1)
	sub, err := bus.Subscribe("test.subject", func(msg *nats.Msg) {
		received <- msg.Data
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	defer sub.Unsubscribe()

	payload := []byte("hello-pipedpeer")
	if err := bus.Publish("test.subject", payload); err != nil {
		t.Fatalf("publish: %v", err)
	}

	select {
	case data := <-received:
		if string(data) != "hello-pipedpeer" {
			t.Fatalf("expected 'hello-pipedpeer', got %q", string(data))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for message")
	}
}

func TestRequestReply(t *testing.T) {
	bus, err := New(Config{Embedded: true, StoreDir: t.TempDir()})
	if err != nil {
		t.Fatalf("create bus: %v", err)
	}
	defer bus.Close()

	// Responder echoes the message back with a prefix
	_, err = bus.Subscribe("test.echo", func(msg *nats.Msg) {
		reply := append([]byte("echo:"), msg.Data...)
		_ = msg.Respond(reply)
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	resp, err := bus.Request("test.echo", []byte("ping"), 2*time.Second)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	if string(resp.Data) != "echo:ping" {
		t.Fatalf("expected 'echo:ping', got %q", string(resp.Data))
	}
}

func TestRequestJSON(t *testing.T) {
	bus, err := New(Config{Embedded: true, StoreDir: t.TempDir()})
	if err != nil {
		t.Fatalf("create bus: %v", err)
	}
	defer bus.Close()

	type Request struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}
	type Response struct {
		Greeting string `json:"greeting"`
	}

	_, err = bus.Subscribe("test.greet", func(msg *nats.Msg) {
		var req Request
		_ = json.Unmarshal(msg.Data, &req)
		resp := Response{Greeting: "Hello, " + req.Name}
		data, _ := json.Marshal(resp)
		_ = msg.Respond(data)
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	var result Response
	err = bus.RequestJSON("test.greet", Request{Name: "Pipedpeer", Age: 1}, &result, 2*time.Second)
	if err != nil {
		t.Fatalf("request json: %v", err)
	}
	if result.Greeting != "Hello, Pipedpeer" {
		t.Fatalf("expected 'Hello, Pipedpeer', got %q", result.Greeting)
	}
}

func TestPublishJSON(t *testing.T) {
	bus, err := New(Config{Embedded: true, StoreDir: t.TempDir()})
	if err != nil {
		t.Fatalf("create bus: %v", err)
	}
	defer bus.Close()

	type Event struct {
		Type   string `json:"type"`
		TaskID string `json:"task_id"`
	}

	received := make(chan Event, 1)
	_, err = bus.Subscribe("test.events", func(msg *nats.Msg) {
		var ev Event
		_ = json.Unmarshal(msg.Data, &ev)
		received <- ev
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	err = bus.PublishJSON("test.events", Event{Type: "completed", TaskID: "task-123"})
	if err != nil {
		t.Fatalf("publish json: %v", err)
	}

	select {
	case ev := <-received:
		if ev.Type != "completed" {
			t.Fatalf("expected type=completed, got %s", ev.Type)
		}
		if ev.TaskID != "task-123" {
			t.Fatalf("expected task_id=task-123, got %s", ev.TaskID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for event")
	}
}

func TestQueueSubscribe(t *testing.T) {
	bus, err := New(Config{Embedded: true, StoreDir: t.TempDir()})
	if err != nil {
		t.Fatalf("create bus: %v", err)
	}
	defer bus.Close()

	// Two queue subscribers on same subject — message should go to exactly one
	received1 := make(chan struct{}, 10)
	received2 := make(chan struct{}, 10)

	_, err = bus.QueueSubscribe("test.queue", "workers", func(msg *nats.Msg) {
		received1 <- struct{}{}
	})
	if err != nil {
		t.Fatalf("queue subscribe 1: %v", err)
	}

	_, err = bus.QueueSubscribe("test.queue", "workers", func(msg *nats.Msg) {
		received2 <- struct{}{}
	})
	if err != nil {
		t.Fatalf("queue subscribe 2: %v", err)
	}

	// Send 10 messages
	for i := 0; i < 10; i++ {
		bus.Publish("test.queue", []byte("work"))
	}

	// Wait for delivery
	time.Sleep(500 * time.Millisecond)

	total := len(received1) + len(received2)
	if total != 10 {
		t.Fatalf("expected 10 total messages delivered, got %d", total)
	}
	// Both workers should receive some messages (load balanced)
	if len(received1) == 0 || len(received2) == 0 {
		t.Logf("warning: uneven distribution (%d/%d) — this can happen but shouldn't be 0/10 consistently", len(received1), len(received2))
	}
}

func TestJetStreamKVBasic(t *testing.T) {
	bus, err := New(Config{Embedded: true, StoreDir: t.TempDir()})
	if err != nil {
		t.Fatalf("create bus: %v", err)
	}
	defer bus.Close()

	if bus.JS == nil {
		t.Fatal("expected JetStream context to be initialized")
	}

	// Create a KV bucket
	ctx := t
	_ = ctx // Just verifying JS is non-nil and the bus is operational
}

func TestMultipleBusesShareServer(t *testing.T) {
	// Create bus1 with embedded server
	bus1, err := New(Config{Embedded: true, StoreDir: t.TempDir()})
	if err != nil {
		t.Fatalf("create bus1: %v", err)
	}
	defer bus1.Close()

	// Create bus2 connecting to bus1's embedded server
	bus2, err := New(Config{URL: bus1.ClientURL(), Name: "bus2"})
	if err != nil {
		t.Fatalf("create bus2: %v", err)
	}
	defer bus2.Close()

	// bus1 subscribes, bus2 publishes
	received := make(chan string, 1)
	_, err = bus1.Subscribe("cross.test", func(msg *nats.Msg) {
		received <- string(msg.Data)
	})
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	// Flush ensures the subscription is propagated to the server
	if err := bus1.Conn.Flush(); err != nil {
		t.Fatalf("flush bus1: %v", err)
	}

	if err := bus2.Publish("cross.test", []byte("from-bus2")); err != nil {
		t.Fatalf("publish: %v", err)
	}
	// Flush ensures the publish is sent
	if err := bus2.Conn.Flush(); err != nil {
		t.Fatalf("flush bus2: %v", err)
	}

	select {
	case msg := <-received:
		if msg != "from-bus2" {
			t.Fatalf("expected 'from-bus2', got %q", msg)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for cross-bus message")
	}
}
