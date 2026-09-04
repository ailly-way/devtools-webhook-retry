package delivery

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/example/devtools-webhook-retry/internal/infrai"
)

type BuildEvent struct {
	EventID    string `json:"event_id"`
	Kind       string `json:"kind"`
	Project    string `json:"project"`
	Revision   string `json:"revision"`
	Status     string `json:"status"`
	WebhookURL string `json:"webhook_url"`
}

type Queue interface {
	QueuePublish(context.Context, any, string) error
	QueueConsume(context.Context, int, int) ([]infrai.Message, error)
	QueueAck(context.Context, string) error
}

type Worker struct {
	Queue      Queue
	HTTPClient *http.Client
	Logger     *slog.Logger
}

func IdempotencyKey(event BuildEvent) string {
	sum := sha256.Sum256([]byte(event.EventID + "\x00" + event.Kind))
	return "build-event-" + hex.EncodeToString(sum[:16])
}

func (w *Worker) Publish(ctx context.Context, event BuildEvent) error {
	if event.EventID == "" || event.Project == "" || event.WebhookURL == "" {
		return fmt.Errorf("event_id, project, and webhook_url are required")
	}
	return w.Queue.QueuePublish(ctx, event, IdempotencyKey(event))
}

func (w *Worker) RunOnce(ctx context.Context) (int, error) {
	messages, err := w.Queue.QueueConsume(ctx, 10, 30)
	if err != nil {
		return 0, err
	}
	delivered := 0
	for _, message := range messages {
		var event BuildEvent
		if err := json.Unmarshal(message.Payload, &event); err != nil {
			w.Logger.Error("invalid queued event", "message_id", message.MessageID, "error", err)
			continue
		}
		if err := w.deliver(ctx, event); err != nil {
			w.Logger.Warn("delivery deferred", "event_id", event.EventID, "project", event.Project, "error", err)
			continue
		}
		if err := w.Queue.QueueAck(ctx, message.MessageID); err != nil {
			return delivered, err
		}
		delivered++
		w.Logger.Info("delivery acknowledged", "event_id", event.EventID, "kind", event.Kind, "project", event.Project)
	}
	return delivered, nil
}

func (w *Worker) deliver(ctx context.Context, event BuildEvent) error {
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, event.WebhookURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Webhook-Event-ID", event.EventID)
	res, err := w.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		return fmt.Errorf("webhook status %d", res.StatusCode)
	}
	return nil
}

func NewWorker(queue Queue, logger *slog.Logger) *Worker {
	return &Worker{Queue: queue, HTTPClient: &http.Client{Timeout: 10 * time.Second}, Logger: logger}
}
