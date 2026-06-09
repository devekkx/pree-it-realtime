package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/devekkx/pree-it-realtime/internal/config"
	"github.com/devekkx/pree-it-realtime/internal/conn"
	"github.com/devekkx/pree-it-realtime/internal/consumer"
	"github.com/devekkx/pree-it-realtime/internal/fanout"
	"github.com/devekkx/pree-it-realtime/internal/handler"
	"github.com/devekkx/pree-it-realtime/internal/presence"
	"github.com/devekkx/pree-it-realtime/internal/router"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.uber.org/zap"
)

func main() {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	cfg, err := config.Load()
	if err != nil {
		logger.Fatal("failed to load config", zap.Error(err))
	}

	// --- OTel tracer ---
	ctx := context.Background()

	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(cfg.OTelEndpoint),
		otlptracegrpc.WithInsecure(),
	)
	if err != nil {
		logger.Fatal("failed to create otel exporter", zap.Error(err))
	}

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String(cfg.ServiceName),
			semconv.DeploymentEnvironmentKey.String(cfg.Environment),
		)),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))
	defer tp.Shutdown(ctx)

	// --- Redis ---
	rdb := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisAddr,
		Password: cfg.RedisPassword,
		DB:       0,
	})
	defer rdb.Close()

	if err := rdb.Ping(ctx).Err(); err != nil {
		logger.Fatal("failed to connect to Redis", zap.Error(err))
	}
	logger.Info("connected to Redis")

	// --- NATS ---
	var nc *nats.Conn
	for i := 0; i < 10; i++ {
		nc, err = nats.Connect(cfg.NatsURL)
		if err == nil {
			break
		}
		logger.Warn("NATS not ready, retrying...", zap.Int("attempt", i+1), zap.Error(err))
		time.Sleep(2 * time.Second)
	}
	if err != nil {
		logger.Fatal("failed to connect to NATS", zap.Error(err))
	}
	defer nc.Close()
	logger.Info("connected to NATS")

	// --- Core components ---
	manager := conn.NewManager(logger)
	dispatcher := fanout.NewDispatcher(manager, logger)
	tracker := presence.NewTracker(rdb, logger)

	// --- Member lookup (calls chat-service internally) ---
	chatServiceURL := getEnvOrDefault("REALTIME_CHAT_SERVICE_URL", "http://chat-service:8084")

	memberLookup := func(ctx context.Context, conversationID uuid.UUID) ([]uuid.UUID, error) {
		type membersResp struct {
			Members []struct {
				UserID uuid.UUID `json:"user_id"`
			} `json:"members"`
		}

		reqBody, _ := json.Marshal(map[string]string{
			"conversation_id": conversationID.String(),
		})

		req, err := http.NewRequestWithContext(ctx, http.MethodGet,
			fmt.Sprintf("%s/api/v1/chat/conversations/%s", chatServiceURL, conversationID.String()),
			bytes.NewReader(reqBody),
		)
		if err != nil {
			return nil, err
		}
		// Internal service call — use a system-level header
		req.Header.Set("X-User-ID", "00000000-0000-0000-0000-000000000000")

		client := &http.Client{Timeout: 5 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("chat-service request failed: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("chat-service returned %d", resp.StatusCode)
		}

		var result membersResp
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return nil, fmt.Errorf("decode members response: %w", err)
		}

		ids := make([]uuid.UUID, len(result.Members))
		for i, m := range result.Members {
			ids[i] = m.UserID
		}
		return ids, nil
	}

	// --- NATS consumer ---
	natsConsumer := consumer.New(nc, dispatcher, memberLookup, logger)
	if err := natsConsumer.Start(ctx); err != nil {
		logger.Fatal("failed to start NATS consumer", zap.Error(err))
	}

	// --- HTTP + WebSocket server ---
	wsHandler := handler.NewWSHandler(manager, dispatcher, tracker, logger)
	r := router.Setup(wsHandler)

	srv := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.Port),
		Handler:      r,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		logger.Info("starting realtime-service", zap.Int("port", cfg.Port))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("server error", zap.Error(err))
		}
	}()

	// --- Graceful shutdown ---
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	logger.Info("shutting down realtime-service...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("server shutdown error", zap.Error(err))
	}

	logger.Info("realtime-service stopped")
}

func getEnvOrDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
