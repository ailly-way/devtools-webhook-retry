package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"time"

	"github.com/example/devtools-webhook-retry/internal/delivery"
	"github.com/example/devtools-webhook-retry/internal/infrai"
)

func main() {
	if err := run(); err != nil {
		slog.Error("service stopped", "error", err)
		os.Exit(1)
	}
}

func run() error {
	key := os.Getenv("INFRAI_API_KEY")
	if key == "" {
		return infrai.ErrMissingAPIKey
	}
	client := infrai.New(key)
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	worker := delivery.NewWorker(client, logger)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if len(os.Args) < 2 {
		return errors.New("command required: setup, serve, or worker")
	}
	switch os.Args[1] {
	case "setup":
		return client.QueueCreate(ctx, "devtools-webhook-delivery-queue")
	case "serve":
		return serve(ctx, worker, logger)
	case "worker":
		return work(ctx, worker, logger)
	default:
		return fmt.Errorf("unknown command %q", os.Args[1])
	}
}

func serve(ctx context.Context, worker *delivery.Worker, logger *slog.Logger) error {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /events", func(res http.ResponseWriter, req *http.Request) {
		var event delivery.BuildEvent
		if err := json.NewDecoder(req.Body).Decode(&event); err != nil {
			http.Error(res, "invalid JSON", http.StatusBadRequest)
			return
		}
		if err := worker.Publish(req.Context(), event); err != nil {
			var apiErr *infrai.APIError
			if errors.As(err, &apiErr) && apiErr.HTTPStatus >= 400 && apiErr.HTTPStatus < 500 {
				http.Error(res, apiErr.Error(), apiErr.HTTPStatus)
				return
			}
			http.Error(res, "event was not accepted", http.StatusBadGateway)
			return
		}
		res.Header().Set("Content-Type", "application/json")
		res.WriteHeader(http.StatusAccepted)
		json.NewEncoder(res).Encode(map[string]string{"event_id": event.EventID, "state": "queued"})
	})

	server := &http.Server{Addr: ":8080", Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		server.Shutdown(shutdownCtx)
	}()
	logger.Info("event receiver listening", "address", server.Addr)
	err := server.ListenAndServe()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

func work(ctx context.Context, worker *delivery.Worker, logger *slog.Logger) error {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		delivered, err := worker.RunOnce(ctx)
		if err != nil {
			logger.Error("queue cycle failed", "error", err)
		}
		if delivered > 0 {
			logger.Info("queue cycle complete", "delivered", delivered)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
