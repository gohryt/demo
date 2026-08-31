package main

import (
	"context"
	"embed"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/gofiber/fiber/v3/middleware/recover"
	"github.com/jackc/pgx/v5/pgxpool"
)

//go:embed schema.sql seed.sql seed_load.sql
var sqlFiles embed.FS

type app struct {
	pool     *pgxpool.Pool
	wake     chan struct{}
	client   *http.Client
	selfURL  string
	timeout  time.Duration
	errA     float64
	toA      float64
	errB     float64
	toB      float64
	issued   sync.Map
	reqLocks sync.Map
}

func main() {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg := loadConfig()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	pool, err := pgxpool.New(ctx, cfg.databaseURL)
	if err != nil {
		slog.Error("db connect", "err", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		slog.Error("db ping", "err", err)
		os.Exit(1)
	}
	if err := migrate(ctx, pool); err != nil {
		slog.Error("migrate", "err", err)
		os.Exit(1)
	}

	a := newApp(pool, cfg)
	fiberApp := a.router()

	ln, err := net.Listen("tcp", cfg.httpAddr)
	if err != nil {
		slog.Error("listen", "err", err)
		os.Exit(1)
	}
	a.selfURL = "http://" + ln.Addr().String()

	go a.runWorker(ctx)

	go func() {
		<-ctx.Done()
		_ = fiberApp.Shutdown()
	}()

	slog.Info("listen", "addr", a.selfURL)
	if err := fiberApp.Listener(ln, fiber.ListenConfig{DisableStartupMessage: true}); err != nil {
		slog.Error("server", "err", err)
		os.Exit(1)
	}
}

func newApp(pool *pgxpool.Pool, cfg config) *app {
	return &app{
		pool:    pool,
		wake:    make(chan struct{}, 1),
		client:  &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 32}},
		timeout: cfg.providerTimeout,
		errA:    cfg.errA,
		toA:     cfg.toA,
		errB:    cfg.errB,
		toB:     cfg.toB,
	}
}

func (a *app) router() *fiber.App {
	r := fiber.New(fiber.Config{AppName: "marketplace"})
	r.Use(recover.New())
	r.Get("/health", a.health)
	r.Post("/api/v1/orders", a.createOrder)
	r.Get("/api/v1/orders/:id", a.getOrder)
	r.Get("/api/v1/catalog", a.catalog)
	r.Post("/webhook/payment", a.paymentWebhook)
	r.Get("/internal/reconciliation", a.reconciliation)
	r.Post("/internal/orders/:id/retry", a.retryOrder)
	r.Post("/stub/provider-a/issue", a.stubIssue("a"))
	r.Post("/stub/provider-b/issue", a.stubIssue("b"))
	return r
}

func (a *app) health(c fiber.Ctx) error {
	if err := a.pool.Ping(c.Context()); err != nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"status": "down"})
	}
	return c.JSON(fiber.Map{"status": "ok"})
}

func (a *app) ping() {
	select {
	case a.wake <- struct{}{}:
	default:
	}
}

type config struct {
	databaseURL     string
	httpAddr        string
	providerTimeout time.Duration
	errA, toA       float64
	errB, toB       float64
}

func loadConfig() config {
	timeout := 2 * time.Second
	if v := os.Getenv("PROVIDER_TIMEOUT"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			timeout = d
		}
	}
	return config{
		databaseURL:     getenv("DATABASE_URL", "postgres://marketplace:marketplace@127.0.0.1:5432/marketplace?sslmode=disable"),
		httpAddr:        getenv("HTTP_ADDR", ":8080"),
		providerTimeout: timeout,
		errA:            envFloat("PROVIDER_A_ERROR_RATE", 0),
		toA:             envFloat("PROVIDER_A_TIMEOUT_RATE", 0),
		errB:            envFloat("PROVIDER_B_ERROR_RATE", 0),
		toB:             envFloat("PROVIDER_B_TIMEOUT_RATE", 0),
	}
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envFloat(key string, def float64) float64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return def
	}
	return f
}

func migrate(ctx context.Context, pool *pgxpool.Pool) error {
	for _, name := range []string{"schema.sql", "seed.sql", "seed_load.sql"} {
		body, err := sqlFiles.ReadFile(name)
		if err != nil {
			return err
		}
		if err := execSimple(ctx, pool, string(body)); err != nil {
			return err
		}
	}
	return nil
}

func execSimple(ctx context.Context, pool *pgxpool.Pool, sql string) error {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		return err
	}
	defer conn.Release()
	_, err = conn.Conn().PgConn().Exec(ctx, sql).ReadAll()
	return err
}
