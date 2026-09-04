package delivery

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/example/devtools-webhook-retry/internal/infrai"
)

type fakeQueue struct {
	messages []infrai.Message
	acked    []string
}

func (q *fakeQueue) QueuePublish(context.Context, any, string) error { return nil }
func (q *fakeQueue) QueueConsume(context.Context, int, int) ([]infrai.Message, error) {
	return q.messages, nil
}
func (q *fakeQueue) QueueAck(_ context.Context, id string) error {
	q.acked = append(q.acked, id)
	return nil
}

func TestRunOnceAcknowledgesOnlyDeliveredEvents(t *testing.T) {
	tests := []struct {
		name      string
		status    int
		wantAcked int
	}{
		{name: "accepted by webhook", status: http.StatusNoContent, wantAcked: 1},
		{name: "rejected by webhook", status: http.StatusServiceUnavailable, wantAcked: 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			target := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
				res.WriteHeader(tt.status)
			}))
			defer target.Close()

			event := BuildEvent{EventID: "evt-42", Kind: "release.published", Project: "compiler", Revision: "a1b2c3", Status: "succeeded", WebhookURL: target.URL}
			payload, _ := json.Marshal(event)
			queue := &fakeQueue{messages: []infrai.Message{{MessageID: "msg-7", Payload: payload}}}
			worker := NewWorker(queue, slog.New(slog.NewTextHandler(io.Discard, nil)))

			delivered, err := worker.RunOnce(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			if delivered != tt.wantAcked || len(queue.acked) != tt.wantAcked {
				t.Fatalf("delivered=%d acknowledgements=%v, want %d", delivered, queue.acked, tt.wantAcked)
			}
		})
	}
}

func TestIdempotencyKeyIsStablePerEventKind(t *testing.T) {
	event := BuildEvent{EventID: "evt-42", Kind: "build.finished"}
	if IdempotencyKey(event) != IdempotencyKey(event) {
		t.Fatal("same event produced different keys")
	}
	changed := event
	changed.Kind = "release.published"
	if IdempotencyKey(event) == IdempotencyKey(changed) {
		t.Fatal("different event kinds produced the same key")
	}
}
