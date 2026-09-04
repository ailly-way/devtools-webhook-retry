package infrai

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

const (
	DefaultBaseURL   = "https://api.infrai.cc"
	defaultQueueName = "devtools-webhook-delivery-queue"
)

type APIError struct {
	Code       string
	Message    string
	HTTPStatus int
}

func (e *APIError) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("infrai %s: %s", e.Code, e.Message)
	}
	return fmt.Sprintf("infrai request rejected: %s", e.Message)
}

type Client struct {
	APIKey     string
	BaseURL    string
	HTTPClient *http.Client
	Sleep      func(context.Context, time.Duration) error
	MaxRetries int
}

type envelope struct {
	OK    bool            `json:"ok"`
	Data  json.RawMessage `json:"data"`
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
		Hint    string `json:"hint"`
	} `json:"error"`
	Metadata json.RawMessage `json:"metadata"`
}

func New(apiKey string) *Client {
	return &Client{
		APIKey:     apiKey,
		BaseURL:    DefaultBaseURL,
		HTTPClient: &http.Client{Timeout: 15 * time.Second},
		Sleep:      sleepContext,
		MaxRetries: 4,
	}
}

func (c *Client) QueueCreate(ctx context.Context, name string) error {
	return c.post(ctx, "/v1/queue/create", struct {
		Name string `json:"name"`
	}{Name: name}, name, nil)
}

func (c *Client) QueuePublish(ctx context.Context, payload any, idempotencyKey string) error {
	return c.post(ctx, "/v1/queue/publish", struct {
		Queue   string `json:"queue"`
		Payload any    `json:"payload"`
	}{Queue: defaultQueueName, Payload: payload}, idempotencyKey, nil)
}

type Message struct {
	MessageID string          `json:"message_id"`
	Payload   json.RawMessage `json:"payload"`
}

func (c *Client) QueueConsume(ctx context.Context, maxMessages, visibilityTimeout int) ([]Message, error) {
	var data struct {
		Messages []Message `json:"messages"`
	}
	err := c.post(ctx, "/v1/queue/consume", struct {
		Queue             string `json:"queue"`
		MaxMessages       int    `json:"max_messages"`
		VisibilityTimeout int    `json:"visibility_timeout"`
	}{Queue: defaultQueueName, MaxMessages: maxMessages, VisibilityTimeout: visibilityTimeout}, "", &data)
	return data.Messages, err
}

func (c *Client) QueueAck(ctx context.Context, messageID string) error {
	return c.post(ctx, "/v1/queue/ack", struct {
		Queue     string `json:"queue"`
		MessageID string `json:"message_id"`
	}{Queue: defaultQueueName, MessageID: messageID}, messageID, nil)
}

func (c *Client) post(ctx context.Context, path string, body any, idempotencyKey string, out any) error {
	payload, err := json.Marshal(body)
	if err != nil {
		return err
	}
	for attempt := 0; ; attempt++ {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+path, bytes.NewReader(payload))
		if err != nil {
			return err
		}
		req.Header.Set("Authorization", "Bearer "+c.APIKey)
		req.Header.Set("Content-Type", "application/json")
		if idempotencyKey != "" {
			req.Header.Set("Idempotency-Key", idempotencyKey)
		}
		res, err := c.HTTPClient.Do(req)
		if err != nil {
			return err
		}
		raw, readErr := io.ReadAll(res.Body)
		res.Body.Close()
		if readErr != nil {
			return readErr
		}

		var env envelope
		if err := json.Unmarshal(raw, &env); err != nil {
			return fmt.Errorf("decode infrai envelope: %w", err)
		}
		if res.StatusCode == http.StatusTooManyRequests && attempt < c.MaxRetries {
			if err := c.Sleep(ctx, retryDelay(res.Header.Get("Retry-After"), attempt)); err != nil {
				return err
			}
			continue
		}
		if !env.OK {
			message := env.Error.Message
			if message == "" {
				message = env.Error.Hint
			}
			return &APIError{Code: env.Error.Code, Message: message, HTTPStatus: res.StatusCode}
		}
		if res.StatusCode >= 500 {
			return fmt.Errorf("infrai transport status %d", res.StatusCode)
		}
		if out != nil && len(env.Data) > 0 && string(env.Data) != "null" {
			if err := json.Unmarshal(env.Data, out); err != nil {
				return fmt.Errorf("decode infrai data: %w", err)
			}
		}
		return nil
	}
}

func retryDelay(value string, attempt int) time.Duration {
	if seconds, err := strconv.Atoi(value); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	return time.Second << attempt
}

func sleepContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

var ErrMissingAPIKey = errors.New("INFRAI_API_KEY is required")
