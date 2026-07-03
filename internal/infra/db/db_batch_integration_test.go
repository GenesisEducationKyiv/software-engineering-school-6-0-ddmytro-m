//go:build integration

package db

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"go.uber.org/zap"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/events"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/outbox"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/logger"
)

func TestMain(m *testing.M) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Log = zap.NewNop()

	container, err := setupPostgresContainer(ctx)
	if err != nil {
		log.Fatalf("critical failure setting up postgres container: %v", err)
	}

	code := m.Run()

	if err := container.Terminate(context.Background()); err != nil {
		log.Fatalf("container termination failure: %v", err)
	}
	os.Exit(code)
}

func setupPostgresContainer(ctx context.Context) (testcontainers.Container, error) {
	pgc, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		return nil, err
	}

	host, err := pgc.Host(ctx)
	if err != nil {
		return nil, err
	}
	port, err := pgc.MappedPort(ctx, "5432/tcp")
	if err != nil {
		return nil, err
	}

	envVars := map[string]string{
		"DB_HOST":     host,
		"DB_PORT":     port.Port(),
		"DB_USER":     "test",
		"DB_PASSWORD": "test",
		"DB_NAME":     "testdb",
		"DB_SSLMODE":  "disable",
	}
	for k, v := range envVars {
		if err := os.Setenv(k, v); err != nil {
			return nil, err
		}
	}

	return pgc, nil
}

type batchLogger struct {
	gormlogger.Interface
	mu    sync.Mutex
	sizes []int64
}

func (l *batchLogger) Trace(ctx context.Context, begin time.Time, fc func() (string, int64), err error) {
	sql, rows := fc()
	if strings.Contains(strings.ToLower(sql), "insert into") && strings.Contains(sql, "outbox_rows") {
		l.mu.Lock()
		l.sizes = append(l.sizes, rows)
		l.mu.Unlock()
	}
	l.Interface.Trace(ctx, begin, fc, err)
}

func TestOutboxInsertTx_ChunksLargeBatches(t *testing.T) {
	orm := Get()
	t.Cleanup(func() {
		if err := orm.Exec("TRUNCATE TABLE outbox_rows RESTART IDENTITY").Error; err != nil {
			t.Fatalf("truncate outbox_rows: %v", err)
		}
	})

	const total = createBatchSize*2 + 500

	evts := make([]outbox.Event, 0, total)
	for i := range total {
		ev, err := outbox.New(events.NewReleaseDetected(events.ReleaseDetected{
			Email:      fmt.Sprintf("sub-%d@example.com", i),
			Repo:       "acme/widgets",
			ReleaseTag: "v1.0.0",
		}))
		if err != nil {
			t.Fatalf("build event: %v", err)
		}
		evts = append(evts, ev)
	}

	spy := &batchLogger{Interface: gormlogger.Discard}
	session := orm.Session(&gorm.Session{Logger: spy})

	if err := outbox.InsertTx(session, evts...); err != nil {
		t.Fatalf("InsertTx: %v", err)
	}

	var count int64
	if err := orm.Model(&outbox.Row{}).Count(&count).Error; err != nil {
		t.Fatalf("count outbox_rows: %v", err)
	}
	if count != int64(total) {
		t.Fatalf("expected %d persisted rows, got %d", total, count)
	}

	if len(spy.sizes) < 3 {
		t.Fatalf("expected the %d-row create to be split into at least 3 INSERT statements, got %d: %v", total, len(spy.sizes), spy.sizes)
	}

	var sum int64
	for _, n := range spy.sizes {
		if n > createBatchSize {
			t.Fatalf("single INSERT affected %d rows, want <= %d (CreateBatchSize)", n, createBatchSize)
		}
		sum += n
	}
	if sum != int64(total) {
		t.Fatalf("sum of batch sizes = %d, want %d", sum, total)
	}
}
