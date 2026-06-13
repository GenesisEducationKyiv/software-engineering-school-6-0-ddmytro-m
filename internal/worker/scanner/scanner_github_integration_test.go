//go:build integration

package scanner

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"

	"fmt"
	"log"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/api/github"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/config"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/db"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/logger"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"

	"golang.org/x/time/rate"

	"go.uber.org/zap"
	gormPostgres "gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"
)

// Scanner-GitHub API integration tests

var testDB *gorm.DB

func TestMain(m *testing.M) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Log = zap.NewNop()

	container, database, err := setupPostgresContainer(ctx)
	if err != nil {
		log.Fatalf("critical failure: %v", err)
	}

	testDB = database

	code := m.Run()

	err = container.Terminate(context.Background())
	if err != nil {
		log.Fatalf("container termination failure: %v", err)
	}

	os.Exit(code)
}

// captureNotifier records every SendNewRelease call for assertion.
type captureNotifier struct {
	releases []notifyCall
	moved    []movedCall
}

type notifyCall struct {
	email   string
	owner   string
	name    string
	tagName string
}

type movedCall struct {
	email string
	owner string
	name  string
}

func (n *captureNotifier) SendNewRelease(
	sub *db.Subscription,
	repo *db.Repository,
	release *github.LatestRelease,
) error {
	n.releases = append(n.releases, notifyCall{
		email:   sub.Email,
		owner:   repo.Owner,
		name:    repo.Name,
		tagName: release.TagName,
	})
	return nil
}

func (n *captureNotifier) SendRepoMoved(
	sub *db.Subscription,
	repo *db.Repository,
) error {
	n.moved = append(n.moved, movedCall{
		email: sub.Email,
		owner: repo.Owner,
		name:  repo.Name,
	})
	return nil
}

// test DB setup
func setupPostgresContainer(ctx context.Context) (testcontainers.Container, *gorm.DB, error) {
	pgc, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("test"),
		postgres.WithPassword("test"),
		postgres.BasicWaitStrategies(),
	)

	if err != nil {
		return nil, nil, err
	}

	dsn, err := pgc.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		return nil, nil, err
	}

	orm, err := gorm.Open(gormPostgres.Open(dsn), &gorm.Config{
		Logger: gormlogger.Default.LogMode(gormlogger.Silent),
	})
	if err != nil {
		return nil, nil, err
	}

	err = orm.AutoMigrate(&db.Repository{}, &db.Subscription{})

	return pgc, orm, err
}

func getCleanDB(t *testing.T) *gorm.DB {
	t.Helper()
	if err := testDB.Exec("TRUNCATE TABLE subscriptions, repositories RESTART IDENTITY CASCADE").Error; err != nil {
		t.Fatalf("failed to truncate db: %v", err)
	}
	return testDB
}

func newScanner(orm *gorm.DB, ghClient *github.Client, rlp RateLimitProvider, notifier Notifier) *Scanner {
	cfg := &config.ScannerConfig{
		Workers:          1,
		QueueSize:        10,
		SafetyBuffer:     0.1,
		MinInterval:      0,
		ProducerInterval: 10 * time.Second,
	}

	s := NewScanner(orm, ghClient, notifier, rlp, cfg)
	s.quota.SetLimit(rate.Inf)
	return s
}

func newGitHubServer(t *testing.T, handler http.HandlerFunc) (*httptest.Server, *github.Client) {
	t.Helper()

	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	client := srv.Client()
	client.Timeout = 5 * time.Second

	transport := client.Transport
	authTransport := github.NewAuthTransport(transport, "test-token")
	client.Transport = authTransport

	c := github.NewClient(
		github.WithHTTPClient(client),
		github.WithBaseURL(srv.URL),
	)

	return srv, c
}

func newGitHubServerWithRateLimits(t *testing.T, handler http.HandlerFunc, rl github.RateLimits) (*httptest.Server, *github.Client, RateLimitProvider) {
	t.Helper()

	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)

	client := srv.Client()
	client.Timeout = 5 * time.Second

	transport := client.Transport
	authTransport := github.NewAuthTransport(transport, "test-token")
	rateLimitTransport := github.NewRateLimitTransport(authTransport, rl)
	client.Transport = rateLimitTransport

	c := github.NewClient(
		github.WithHTTPClient(client),
		github.WithBaseURL(srv.URL),
	)

	return srv, c, rateLimitTransport
}

func seedRepo(t *testing.T, orm *gorm.DB, githubID int64, owner, name, lastTag, etag string) *db.Repository {
	t.Helper()
	repo := &db.Repository{
		GitHubID: githubID,
		Owner:    owner,
		Name:     name,
		Status:   db.StatusIdle,
		LastRelease: db.Release{
			TagName: lastTag,
			ETag:    etag,
		},
	}
	if err := orm.Create(repo).Error; err != nil {
		t.Fatalf("seedRepo: %v", err)
	}

	return repo
}

func seedSubscription(t *testing.T, orm *gorm.DB, repoID uint, email string) *db.Subscription {
	t.Helper()
	sub := &db.Subscription{
		Email:        email,
		RepositoryID: repoID,
		Status:       db.StatusActive,
		ConfirmToken: fmt.Sprintf("tok-%d-%s", repoID, email),
		APIToken:     fmt.Sprintf("api-%d-%s", repoID, email),
	}
	if err := orm.Create(sub).Error; err != nil {
		t.Fatalf("seedSubscription: %v", err)
	}
	return sub
}

func repoAndReleaseHandler(owner, name string, repoBody, releaseBody string, repoStatus, releaseStatus int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		repoPath := fmt.Sprintf("/repos/%s/%s", owner, name)
		releasePath := fmt.Sprintf("/repos/%s/%s/releases/latest", owner, name)

		switch r.URL.Path {
		case repoPath:
			w.Header().Set("ETag", `"etag-repo"`)
			w.WriteHeader(repoStatus)
			if repoBody != "" {
				if _, err := w.Write([]byte(repoBody)); err != nil {
					log.Printf("failed to write repo response: %v", err)
				}
			}
		case releasePath:
			w.Header().Set("ETag", `"etag-release-new"`)
			w.Header().Set("X-RateLimit-Limit", "5000")
			w.Header().Set("X-RateLimit-Remaining", "4999")
			w.Header().Set("X-RateLimit-Reset", "9999999999")
			w.WriteHeader(releaseStatus)
			if releaseBody != "" {
				if _, err := w.Write([]byte(releaseBody)); err != nil {
					log.Printf("failed to write release response: %v", err)
				}
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}
}

func TestProcessRepo_NewRelease_NotifiesSubscribersAndPersists(t *testing.T) {
	dbConn := getCleanDB(t)
	notifier := &captureNotifier{}

	repo := seedRepo(t, dbConn, 42, "golang", "go", "v1.21.0", `"etag-old"`)
	seedSubscription(t, dbConn, repo.ID, "alice@example.com")
	seedSubscription(t, dbConn, repo.ID, "bob@example.com")

	_, ghClient := newGitHubServer(t, repoAndReleaseHandler(
		"golang", "go",
		`{"id":42,"full_name":"golang/go"}`,
		`{"id":2,"tag_name":"v1.22.0","html_url":"https://github.com/golang/go/releases/tag/v1.22.0"}`,
		http.StatusOK, http.StatusOK,
	))

	rl := github.RateLimits{
		Limit:     5000,
		Remaining: 5000,
		ResetAt:   time.Now().Add(time.Hour),
	}
	s := newScanner(dbConn, ghClient, github.NewRateLimitTransport(nil, rl), notifier)
	s.processor.ProcessRepo(context.Background(), repo)

	// two notifications — one per active subscriber
	if len(notifier.releases) != 2 {
		t.Fatalf("expected 2 notifications, got %d", len(notifier.releases))
	}
	emails := map[string]bool{}
	for _, c := range notifier.releases {
		emails[c.email] = true
		if c.tagName != "v1.22.0" {
			t.Errorf("notification tagName = %q, want v1.22.0", c.tagName)
		}
	}
	if !emails["alice@example.com"] || !emails["bob@example.com"] {
		t.Errorf("missing expected recipients: %v", emails)
	}

	// tag and etag must be persisted
	var updated db.Repository
	dbConn.First(&updated, repo.ID)
	if updated.LastRelease.TagName != "v1.22.0" {
		t.Errorf("persisted TagName = %q, want v1.22.0", updated.LastRelease.TagName)
	}
	if updated.LastRelease.ETag != `"etag-release-new"` {
		t.Errorf("persisted ETag = %q, want \"etag-release-new\"", updated.LastRelease.ETag)
	}
	if updated.ETag != `"etag-repo"` {
		t.Errorf("persisted repo ETag = %q, want \"etag-repo\"", updated.ETag)
	}
	if updated.Status != db.StatusIdle {
		t.Errorf("status = %q, want idle after processing", updated.Status)
	}
}

func TestProcessRepo_RepoMoved_NotifiesAndUnsubscribes(t *testing.T) {
	dbConn := getCleanDB(t)
	notifier := &captureNotifier{}

	// original repo ID is 42
	repo := seedRepo(t, dbConn, 42, "golang", "go", "v1.21.0", "")
	seedSubscription(t, dbConn, repo.ID, "alice@example.com")
	seedSubscription(t, dbConn, repo.ID, "bob@example.com")

	_, ghClient := newGitHubServer(t, func(w http.ResponseWriter, r *http.Request) {
		repoPath := fmt.Sprintf("/repos/%s/%s", repo.Owner, repo.Name)
		if r.URL.Path == repoPath {
			w.WriteHeader(http.StatusOK)
			// return a different ID (99 instead of 42) to trigger moved logic
			if _, err := w.Write([]byte(`{"id":99,"full_name":"golang/go"}`)); err != nil {
				t.Errorf("failed to write response: %v", err)
			}
			return
		}
		t.Errorf("unexpected request path: %s", r.URL.Path)
		w.WriteHeader(http.StatusInternalServerError)
	})

	s := newScanner(dbConn, ghClient, github.NewRateLimitTransport(nil, github.RateLimits{}), notifier)
	s.processor.ProcessRepo(context.Background(), repo)

	// should trigger SendRepoMoved for active subscribers
	if len(notifier.moved) != 2 {
		t.Fatalf("expected 2 moved notifications, got %d", len(notifier.moved))
	}
	emails := map[string]bool{}
	for _, c := range notifier.moved {
		emails[c.email] = true
	}
	if !emails["alice@example.com"] || !emails["bob@example.com"] {
		t.Errorf("missing expected recipients: %v", emails)
	}

	// should NOT trigger SendNewRelease
	if len(notifier.releases) != 0 {
		t.Errorf("expected 0 release notifications, got %d", len(notifier.releases))
	}

	// subscriptions should be marked as unsubscribed
	var unsubSubs int64
	dbConn.Model(&db.Subscription{}).Where("repository_id = ? AND status = ?", repo.ID, db.StatusUnsubscribed).Count(&unsubSubs)
	if unsubSubs != 2 {
		t.Errorf("expected 2 unsubscribed subscriptions, got %d", unsubSubs)
	}
}

func TestProcessRepo_304_NoNotificationNoTagChange(t *testing.T) {
	dbConn := getCleanDB(t)
	notifier := &captureNotifier{}

	repo := seedRepo(t, dbConn, 42, "golang", "go", "v1.21.0", `"etag-v1"`)
	seedSubscription(t, dbConn, repo.ID, "alice@example.com")

	_, ghClient := newGitHubServer(t, func(w http.ResponseWriter, r *http.Request) {
		// both repo check and release check return 304
		w.WriteHeader(http.StatusNotModified)
	})

	s := newScanner(dbConn, ghClient, github.NewRateLimitTransport(nil, github.RateLimits{}), notifier)
	s.processor.ProcessRepo(context.Background(), repo)

	if len(notifier.releases) != 0 {
		t.Errorf("expected 0 notifications on 304, got %d", len(notifier.releases))
	}

	var updated db.Repository
	dbConn.First(&updated, repo.ID)
	if updated.LastRelease.TagName != "v1.21.0" {
		t.Errorf("tag should be unchanged on 304, got %q", updated.LastRelease.TagName)
	}
}

func TestProcessRepo_SameTag_NoNotification(t *testing.T) {
	dbConn := getCleanDB(t)
	notifier := &captureNotifier{}

	repo := seedRepo(t, dbConn, 42, "golang", "go", "v1.21.0", "")

	_, ghClient := newGitHubServer(t, repoAndReleaseHandler(
		"golang", "go",
		`{"id":42,"full_name":"golang/go"}`,
		`{"tag_name":"v1.21.0"}`,
		http.StatusOK, http.StatusOK,
	))

	s := newScanner(dbConn, ghClient, github.NewRateLimitTransport(nil, github.RateLimits{}), notifier)
	s.processor.ProcessRepo(context.Background(), repo)

	if len(notifier.releases) != 0 {
		t.Errorf("expected 0 notifications when tag unchanged, got %d", len(notifier.releases))
	}
}

func TestProcessRepo_RepoNotFound_SkipsRelease(t *testing.T) {
	dbConn := getCleanDB(t)
	notifier := &captureNotifier{}

	repo := seedRepo(t, dbConn, 42, "golang", "go", "v1.21.0", "")
	seedSubscription(t, dbConn, repo.ID, "alice@example.com")

	_, ghClient := newGitHubServer(t, func(w http.ResponseWriter, r *http.Request) {
		repoPath := fmt.Sprintf("/repos/%s/%s", repo.Owner, repo.Name)
		if r.URL.Path == repoPath {
			w.WriteHeader(http.StatusNotFound)
			if _, err := w.Write([]byte(`{"message":"Not Found"}`)); err != nil {
				t.Errorf("failed to write response: %v", err)
			}
			return
		}
		t.Error("release endpoint should not be called when repo returns 404")
		w.WriteHeader(http.StatusInternalServerError)
	})

	s := newScanner(dbConn, ghClient, github.NewRateLimitTransport(nil, github.RateLimits{}), notifier)
	s.processor.ProcessRepo(context.Background(), repo)

	if len(notifier.releases) != 0 {
		t.Errorf("expected 0 notifications when repo is 404, got %d", len(notifier.releases))
	}
	// subscriptions left intact — repo may just be temporarily private
	var activeSubs int64
	dbConn.Model(&db.Subscription{}).
		Where("repository_id = ? AND status = ?", repo.ID, db.StatusActive).
		Count(&activeSubs)
	if activeSubs != 1 {
		t.Errorf("expected subscriptions to remain active on 404, got %d active", activeSubs)
	}
}

func TestProcessRepo_RepoCheckRateLimited_FreezesLimiterSkipsRelease(t *testing.T) {
	dbConn := getCleanDB(t)
	notifier := &captureNotifier{}

	repo := seedRepo(t, dbConn, 42, "golang", "go", "v1.21.0", "")

	_, ghClient := newGitHubServer(t, func(w http.ResponseWriter, r *http.Request) {
		repoPath := fmt.Sprintf("/repos/%s/%s", repo.Owner, repo.Name)
		if r.URL.Path == repoPath {
			w.WriteHeader(http.StatusTooManyRequests)
			if _, err := w.Write([]byte(`{"message":"rate limit exceeded"}`)); err != nil {
				t.Errorf("failed to write response: %v", err)
			}
			return
		}
		t.Error("release endpoint should not be called when repo check is rate-limited")
		w.WriteHeader(http.StatusInternalServerError)
	})

	s := newScanner(dbConn, ghClient, github.NewRateLimitTransport(nil, github.RateLimits{}), notifier)
	s.processor.ProcessRepo(context.Background(), repo)

	if s.quota.Limit() != 0 {
		t.Errorf("limiter should be frozen (0) after 429 on repo check, got %v", s.quota.Limit())
	}
	if len(notifier.releases) != 0 {
		t.Errorf("expected 0 notifications, got %d", len(notifier.releases))
	}
}

func TestProcessRepo_RepoETagCachedOn304(t *testing.T) {
	dbConn := getCleanDB(t)

	// repo already has an ETag stored from a previous 200
	repo := seedRepo(t, dbConn, 42, "golang", "go", "v1.21.0", "")
	repo.ETag = `"etag-repo-stored"`
	if err := dbConn.Model(repo).Update("e_tag", repo.ETag).Error; err != nil {
		t.Fatalf("failed to update repo: %v", err)
	}

	releaseCallCount := 0
	_, ghClient := newGitHubServer(t, func(w http.ResponseWriter, r *http.Request) {
		repoPath := fmt.Sprintf("/repos/%s/%s", repo.Owner, repo.Name)
		releasePath := fmt.Sprintf("/repos/%s/%s/releases/latest", repo.Owner, repo.Name)
		switch r.URL.Path {
		case repoPath:
			if r.Header.Get("If-None-Match") != `"etag-repo-stored"` {
				t.Errorf("expected If-None-Match = \"etag-repo-stored\", got %q",
					r.Header.Get("If-None-Match"))
			}
			w.WriteHeader(http.StatusNotModified)
		case releasePath:
			releaseCallCount++
			w.WriteHeader(http.StatusNotModified)
		}
	})

	s := newScanner(dbConn, ghClient, github.NewRateLimitTransport(nil, github.RateLimits{}), &captureNotifier{})
	s.processor.ProcessRepo(context.Background(), repo)

	// repo 304 → proceed to release check; confirm the ETag was sent
	if releaseCallCount != 1 {
		t.Errorf("expected release endpoint to be called once after repo 304, got %d", releaseCallCount)
	}
}

func TestProcessRepo_403_FreezesLimiter(t *testing.T) {
	dbConn := getCleanDB(t)

	repo := seedRepo(t, dbConn, 42, "golang", "go", "v1.21.0", "")

	_, ghClient := newGitHubServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		if _, err := w.Write([]byte(`{"message":"rate limit exceeded"}`)); err != nil {
			t.Errorf("failed to write response: %v", err)
		}
	})

	s := newScanner(dbConn, ghClient, nil, &captureNotifier{})
	s.processor.ProcessRepo(context.Background(), repo)

	if s.quota.Limit() != 0 {
		t.Errorf("limiter.Limit() = %v, want 0 after 403", s.quota.Limit())
	}
}

func TestProcessRepo_OnlyActiveSubscriptionsNotified(t *testing.T) {
	dbConn := getCleanDB(t)
	notifier := &captureNotifier{}

	repo := seedRepo(t, dbConn, 42, "golang", "go", "v1.21.0", "")

	active := seedSubscription(t, dbConn, repo.ID, "active@example.com")
	_ = active

	pending := seedSubscription(t, dbConn, repo.ID, "pending@example.com")
	if err := dbConn.Model(pending).Update("status", db.StatusPending).Error; err != nil {
		t.Fatalf("failed to update pending subscription: %v", err)
	}

	unsub := seedSubscription(t, dbConn, repo.ID, "unsub@example.com")
	if err := dbConn.Model(unsub).Update("status", db.StatusUnsubscribed).Error; err != nil {
		t.Fatalf("failed to update unsubscribed subscription: %v", err)
	}

	_, ghClient := newGitHubServer(t, repoAndReleaseHandler(
		"golang", "go",
		`{"id":42,"full_name":"golang/go"}`,
		`{"id":2,"tag_name":"v1.22.0"}`,
		http.StatusOK, http.StatusOK,
	))

	s := newScanner(dbConn, ghClient, github.NewRateLimitTransport(nil, github.RateLimits{}), notifier)
	s.processor.ProcessRepo(context.Background(), repo)

	if len(notifier.releases) != 1 {
		t.Fatalf("expected 1 notification (active only), got %d", len(notifier.releases))
	}
	if notifier.releases[0].email != "active@example.com" {
		t.Errorf("notification sent to %q, want active@example.com", notifier.releases[0].email)
	}
}

func TestProcessRepo_StatusRestoredToIdleAfterProcessing(t *testing.T) {
	dbConn := getCleanDB(t)

	repo := seedRepo(t, dbConn, 42, "golang", "go", "v1.21.0", "")
	// simulate that it was picked up by the producer
	if err := dbConn.Model(repo).Update("status", db.StatusProcessing).Error; err != nil {
		t.Fatalf("failed to update repo status: %v", err)
	}

	_, ghClient := newGitHubServer(t, repoAndReleaseHandler(
		"golang", "go",
		`{"id":42,"full_name":"golang/go"}`,
		`{"tag_name":"v1.21.0"}`,
		http.StatusOK, http.StatusOK,
	))

	s := newScanner(dbConn, ghClient, github.NewRateLimitTransport(nil, github.RateLimits{}), &captureNotifier{})
	s.processor.ProcessRepo(context.Background(), repo)

	var updated db.Repository
	dbConn.First(&updated, repo.ID)
	if updated.Status != db.StatusIdle {
		t.Errorf("status = %q after processRepo, want idle", updated.Status)
	}
}

func TestProduce_RateLimitLow_LimiterSetToZero(t *testing.T) {
	dbConn := getCleanDB(t)

	reset := time.Now().Add(time.Hour)
	rl := github.RateLimits{
		Limit:     5000,
		Remaining: 400, // less than 10%
		ResetAt:   reset,
	}

	_, ghClient, rlp := newGitHubServerWithRateLimits(t, func(w http.ResponseWriter, r *http.Request) {}, rl)

	s := newScanner(dbConn, ghClient, rlp, &captureNotifier{})
	s.producer.Produce(context.Background(), s.repoQueue)
	s.producer.Produce(context.Background(), s.repoQueue)
	s.producer.Produce(context.Background(), s.repoQueue)
	s.producer.Produce(context.Background(), s.repoQueue)

	if s.quota.Limit() != 0 {
		t.Errorf("limiter should be 0 when usable requests <= 0, got %v", s.quota.Limit())
	}
	if len(s.repoQueue) != 0 {
		t.Errorf("queue should be empty when usable requests <= 0, got %d", len(s.repoQueue))
	}
}

func TestProduce_RateLimitHealthy_LimiterPositive(t *testing.T) {
	dbConn := getCleanDB(t)

	reset := time.Now().Add(time.Hour)
	rl := github.RateLimits{
		Limit:     5000,
		Remaining: 4000,
		ResetAt:   reset,
	}

	_, ghClient, rlp := newGitHubServerWithRateLimits(t, func(w http.ResponseWriter, r *http.Request) {}, rl)

	s := newScanner(dbConn, ghClient, rlp, &captureNotifier{})
	s.producer.Produce(context.Background(), s.repoQueue)

	if s.quota.Limit() <= 0 {
		t.Errorf("limiter should be positive with healthy rate limits, got %v", s.quota.Limit())
	}
}

func TestProduce_RetryAfterInFuture_LimiterFrozen(t *testing.T) {
	dbConn := getCleanDB(t)

	rl := github.RateLimits{
		Limit:      5000,
		Remaining:  0,
		ResetAt:    time.Now().Add(time.Hour),
		RetryAfter: time.Now().Add(10 * time.Minute),
	}

	_, ghClient, rlp := newGitHubServerWithRateLimits(t, func(w http.ResponseWriter, r *http.Request) {}, rl)

	s := newScanner(dbConn, ghClient, rlp, &captureNotifier{})
	s.producer.Produce(context.Background(), s.repoQueue)

	if s.quota.Limit() != 0 {
		t.Errorf("limiter should be 0 during RetryAfter backoff, got %v", s.quota.Limit())
	}
}

func TestProduce_RpsCapAt10(t *testing.T) {
	dbConn := getCleanDB(t)

	rl := github.RateLimits{
		Limit:     1_000_000,
		Remaining: 999_000,
		ResetAt:   time.Now().Add(10 * time.Second),
	}

	_, ghClient, rlp := newGitHubServerWithRateLimits(t, func(w http.ResponseWriter, r *http.Request) {}, rl)

	s := newScanner(dbConn, ghClient, rlp, &captureNotifier{})
	s.producer.Produce(context.Background(), s.repoQueue)

	if float64(s.quota.Limit()) > 5.0 {
		t.Errorf("limiter should be capped at 5 repos per second (10 API rps), got %v", s.quota.Limit())
	}
}

func TestHandleNewRelease_NotifiesAllActiveSubscribers(t *testing.T) {
	dbConn := getCleanDB(t)
	notifier := &captureNotifier{}

	repo := seedRepo(t, dbConn, 42, "torvalds", "linux", "v6.7", "")
	seedSubscription(t, dbConn, repo.ID, "one@example.com")
	seedSubscription(t, dbConn, repo.ID, "two@example.com")

	s := newScanner(dbConn, github.NewClient(), github.NewRateLimitTransport(nil, github.RateLimits{}), notifier)
	p := s.processor.(*domainRepoProcessor)
	p.handleNewRelease(repo, &github.LatestRelease{TagName: "v6.8", URL: "https://github.com/torvalds/linux/releases/tag/v6.8"})

	if len(notifier.releases) != 2 {
		t.Fatalf("expected 2 notifications, got %d", len(notifier.releases))
	}
	for _, c := range notifier.releases {
		if c.tagName != "v6.8" {
			t.Errorf("notification tagName = %q, want v6.8", c.tagName)
		}
		if c.owner != "torvalds" || c.name != "linux" {
			t.Errorf("notification repo = %q/%q, want torvalds/linux", c.owner, c.name)
		}
	}
}

func TestRecover_ResetsProcessingToIdle(t *testing.T) {
	dbConn := getCleanDB(t)

	repo1 := seedRepo(t, dbConn, 1, "org", "repo1", "v1", "")
	if err := dbConn.Model(repo1).Update("status", db.StatusProcessing).Error; err != nil {
		t.Fatalf("failed to update repo1 status: %v", err)
	}

	repo2 := seedRepo(t, dbConn, 2, "org", "repo2", "v1", "")
	if err := dbConn.Model(repo2).Update("status", db.StatusIdle).Error; err != nil {
		t.Fatalf("failed to update repo2 status: %v", err)
	}

	s := newScanner(dbConn, github.NewClient(), github.NewRateLimitTransport(nil, github.RateLimits{}), &captureNotifier{})
	s.recover()

	var updated1 db.Repository
	dbConn.First(&updated1, repo1.ID)
	if updated1.Status != db.StatusIdle {
		t.Errorf("expected repo1 to be idle, got %q", updated1.Status)
	}

	var updated2 db.Repository
	dbConn.First(&updated2, repo2.ID)
	if updated2.Status != db.StatusIdle {
		t.Errorf("expected repo2 to remain idle, got %q", updated2.Status)
	}
}

func TestProduce_MultipleRepos_CorrectBatching(t *testing.T) {
	dbConn := getCleanDB(t)

	// setup healthy rate limits to allow batching
	reset := time.Now().Add(time.Hour)
	rl := github.RateLimits{
		Limit:     5000,
		Remaining: 5000,
		ResetAt:   reset,
	}

	_, ghClient, rlp := newGitHubServerWithRateLimits(t, func(w http.ResponseWriter, r *http.Request) {}, rl)

	// Seed 5 repositories
	for i := range 5 {
		seedRepo(t, dbConn, int64(i+1), "owner", fmt.Sprintf("repo-%d", i), "v1.0.0", "")
	}

	cfg := &config.ScannerConfig{
		Workers:          1,
		QueueSize:        3, // queue size is smaller than the total repos
		SafetyBuffer:     0.1,
		MinInterval:      0,
		ProducerInterval: 10 * time.Second,
	}
	s := NewScanner(dbConn, ghClient, &captureNotifier{}, rlp, cfg)
	s.quota.SetLimit(rate.Inf)

	s.producer.Produce(context.Background(), s.repoQueue)

	if len(s.repoQueue) != 3 {
		t.Errorf("expected 3 repos in queue, got %d", len(s.repoQueue))
	}

	var processingCount int64
	dbConn.Model(&db.Repository{}).Where("status = ?", db.StatusProcessing).Count(&processingCount)
	if processingCount != 3 {
		t.Errorf("expected 3 repos to be in processing status, got %d", processingCount)
	}
}

func TestScanner_Integration_MultipleSubscribersDifferentRepos(t *testing.T) {
	dbConn := getCleanDB(t)
	notifier := &captureNotifier{}

	repo1 := seedRepo(t, dbConn, 1, "org", "project-a", "v1.0.0", "")
	repo2 := seedRepo(t, dbConn, 2, "org", "project-b", "v2.0.0", "")

	seedSubscription(t, dbConn, repo1.ID, "user1@example.com")
	seedSubscription(t, dbConn, repo2.ID, "user1@example.com")
	seedSubscription(t, dbConn, repo1.ID, "user2@example.com")
	seedSubscription(t, dbConn, repo2.ID, "user3@example.com")

	_, ghClient := newGitHubServer(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-RateLimit-Limit", "5000")
		w.Header().Set("X-RateLimit-Remaining", "4999")
		w.Header().Set("X-RateLimit-Reset", "9999999999")

		switch r.URL.Path {
		case "/repos/org/project-a":
			w.WriteHeader(http.StatusOK)
			if _, err := w.Write([]byte(`{"id":1,"full_name":"org/project-a"}`)); err != nil {
				t.Errorf("failed to write response: %v", err)
			}
		case "/repos/org/project-b":
			w.WriteHeader(http.StatusOK)
			if _, err := w.Write([]byte(`{"id":2,"full_name":"org/project-b"}`)); err != nil {
				t.Errorf("failed to write response: %v", err)
			}
		case "/repos/org/project-a/releases/latest":
			w.Header().Set("ETag", `"etag-a-new"`)
			w.WriteHeader(http.StatusOK)
			if _, err := w.Write([]byte(`{"id":1,"tag_name":"v1.1.0","html_url":"https://github.com/org/project-a/releases/tag/v1.1.0"}`)); err != nil {
				t.Errorf("failed to write response: %v", err)
			}
		case "/repos/org/project-b/releases/latest":
			w.Header().Set("ETag", `"etag-b-new"`)
			w.WriteHeader(http.StatusOK)
			if _, err := w.Write([]byte(`{"id":2,"tag_name":"v2.1.0","html_url":"https://github.com/org/project-b/releases/tag/v2.1.0"}`)); err != nil {
				t.Errorf("failed to write response: %v", err)
			}
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})

	s := newScanner(dbConn, ghClient, github.NewRateLimitTransport(nil, github.RateLimits{}), notifier)
	s.processor.ProcessRepo(context.Background(), repo1)
	s.processor.ProcessRepo(context.Background(), repo2)

	if len(notifier.releases) != 4 {
		t.Fatalf("expected 4 notifications total, got %d", len(notifier.releases))
	}

	counts := map[string]int{}
	for _, call := range notifier.releases {
		key := fmt.Sprintf("%s:%s:%s", call.email, call.name, call.tagName)
		counts[key]++
	}

	for _, ec := range []string{
		"user1@example.com:project-a:v1.1.0",
		"user1@example.com:project-b:v2.1.0",
		"user2@example.com:project-a:v1.1.0",
		"user3@example.com:project-b:v2.1.0",
	} {
		if counts[ec] != 1 {
			t.Errorf("expected 1 notification for %q, got %d", ec, counts[ec])
		}
	}
}
