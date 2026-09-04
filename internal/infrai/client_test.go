package infrai

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestClientDecodesBusinessRejectionBeforeHTTPStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		res.Header().Set("Content-Type", "application/json")
		res.WriteHeader(http.StatusBadRequest)
		res.Write([]byte(`{"ok":false,"data":null,"error":{"message":"event rejected"},"metadata":{}}`))
	}))
	defer server.Close()

	client := New("test-key")
	client.BaseURL = server.URL
	err := client.QueuePublish(context.Background(), map[string]string{"event_id": "evt-1"}, "stable-key")
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Message != "event rejected" || apiErr.HTTPStatus != http.StatusBadRequest {
		t.Fatalf("got %#v, want decoded APIError", err)
	}
}

func TestClientRetriesRateLimitAndHonorsRetryAfter(t *testing.T) {
	attempts := 0
	server := httptest.NewServer(http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		attempts++
		res.Header().Set("Content-Type", "application/json")
		if attempts == 1 {
			res.Header().Set("Retry-After", "3")
			res.WriteHeader(http.StatusTooManyRequests)
			res.Write([]byte(`{"ok":false,"data":null,"error":{"message":"rate limited"},"metadata":{}}`))
			return
		}
		res.Write([]byte(`{"ok":true,"data":{},"error":null,"metadata":{}}`))
	}))
	defer server.Close()

	client := New("test-key")
	client.BaseURL = server.URL
	var delays []time.Duration
	client.Sleep = func(_ context.Context, delay time.Duration) error {
		delays = append(delays, delay)
		return nil
	}
	if err := client.QueuePublish(context.Background(), "payload", "stable-key"); err != nil {
		t.Fatal(err)
	}
	if attempts != 2 || len(delays) != 1 || delays[0] != 3*time.Second {
		t.Fatalf("attempts=%d delays=%v", attempts, delays)
	}
}
