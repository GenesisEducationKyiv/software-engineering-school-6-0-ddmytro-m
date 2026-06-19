//go:build integration

package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"os/signal"
	"syscall"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"go.uber.org/zap"
	gormPostgres "gorm.io/driver/postgres"
	"gorm.io/gorm"
	gormlogger "gorm.io/gorm/logger"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/api/github"
	infraDB "github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/db"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/logger"
)

var testDB *gorm.DB

func TestMain(m *testing.M) {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Log = zap.NewNop()

	container, orm, err := setupPostgresContainer(ctx)
	if err != nil {
		log.Fatalf("critical failure setting up postgres container: %v", err)
	}

	testDB = orm

	code := m.Run()

	if err := container.Terminate(context.Background()); err != nil {
		log.Fatalf("container termination failure: %v", err)
	}

	os.Exit(code)
}

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

	err = orm.AutoMigrate(&infraDB.Repository{}, &infraDB.Subscription{})

	return pgc, orm, err
}

func getCleanDB(t *testing.T) *gorm.DB {
	t.Helper()
	testDB.Exec("TRUNCATE TABLE subscriptions, repositories RESTART IDENTITY CASCADE")
	return testDB
}

type mockEmailSender struct {
	sentTokens map[string]string
	err        error
}

func (m *mockEmailSender) SendEmailVerification(email, token string) error {
	if m.err != nil {
		return m.err
	}
	if m.sentTokens == nil {
		m.sentTokens = make(map[string]string)
	}
	m.sentTokens[email] = token
	return nil
}

func setupTestEnv(t *testing.T, db *gorm.DB) (*gin.Engine, *httptest.Server, *mockEmailSender) {
	t.Helper()
	gin.SetMode(gin.TestMode)

	ghServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/repos/testowner/testrepo" {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"id": 12345, "full_name": "testowner/testrepo"}`))
			return
		}
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"message": "Not Found"}`))
	}))

	ghClient := github.NewClient(
		github.WithBaseURL(ghServer.URL),
		github.WithHTTPClient(ghServer.Client()),
	)

	r := gin.Default()
	emailSender := &mockEmailSender{sentTokens: make(map[string]string)}
	store := infraDB.NewSubscriptionStore(db)
	handler := NewSubscriptionHandler(store, ghClient, emailSender)
	handler.RegisterRoutes(r.Group("/"))

	return r, ghServer, emailSender
}

func performRequest(r http.Handler, method, path string, body []byte, headers map[string]string) *httptest.ResponseRecorder {
	req, _ := http.NewRequest(method, path, bytes.NewBuffer(body))
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func TestSubscribe_Success(t *testing.T) {
	db := getCleanDB(t)
	router, ghServer, emailSender := setupTestEnv(t, db)
	defer ghServer.Close()

	testEmail := "user@example.com"
	testRepo := "testowner/testrepo"

	subReq, _ := json.Marshal(SubscribeRequest{Email: testEmail, Repo: testRepo})
	w := performRequest(router, http.MethodPost, "/subscribe", subReq, map[string]string{"Content-Type": "application/json"})
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d. Body: %s", w.Code, w.Body.String())
	}

	var sub infraDB.Subscription
	db.First(&sub, "email = ?", testEmail)

	if token, ok := emailSender.sentTokens[testEmail]; !ok || token != sub.ConfirmToken {
		t.Fatalf("expected email verification to be sent with token %s, got %s", sub.ConfirmToken, token)
	}
}

func TestConfirm_Success(t *testing.T) {
	db := getCleanDB(t)
	router, ghServer, _ := setupTestEnv(t, db)
	defer ghServer.Close()

	repo := infraDB.Repository{Owner: "testowner", Name: "testrepo", GitHubID: 12345}
	db.Create(&repo)

	sub := infraDB.Subscription{
		Email:        "user@example.com",
		RepositoryID: repo.ID,
		Status:       infraDB.StatusPending,
		ConfirmToken: "11111222223333344444555556666677",
	}
	db.Create(&sub)

	w := performRequest(router, http.MethodGet, "/confirm/"+sub.ConfirmToken, nil, nil)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d. Body: %s", w.Code, w.Body.String())
	}

	apiToken := w.Header().Get("X-Api-Token")
	if apiToken == "" {
		t.Fatalf("expected X-Api-Token header, got none")
	}

	var updatedSub infraDB.Subscription
	db.First(&updatedSub, sub.ID)
	if updatedSub.Status != infraDB.StatusActive {
		t.Fatalf("expected status %s, got %s", infraDB.StatusActive, updatedSub.Status)
	}
}

func TestGetSubscriptions_Success(t *testing.T) {
	db := getCleanDB(t)
	router, ghServer, _ := setupTestEnv(t, db)
	defer ghServer.Close()

	repo := infraDB.Repository{Owner: "testowner", Name: "testrepo", GitHubID: 12345}
	db.Create(&repo)

	sub := infraDB.Subscription{
		Email:        "user@example.com",
		RepositoryID: repo.ID,
		Status:       infraDB.StatusActive,
		ConfirmToken: "11111222223333344444555556666677",
		APIToken:     "12345678901234567890123456789012",
	}
	db.Create(&sub)

	w := performRequest(router, http.MethodGet, "/subscriptions?email="+sub.Email, nil, map[string]string{
		"Authorization": "Bearer " + sub.APIToken,
	})
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d. Body: %s", w.Code, w.Body.String())
	}

	var items []SubscriptionItem
	if err := json.Unmarshal(w.Body.Bytes(), &items); err != nil {
		t.Fatalf("failed to parse response body: %v", err)
	}

	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}
	if items[0].Repo != "testowner/testrepo" {
		t.Errorf("expected repo testowner/testrepo, got %s", items[0].Repo)
	}
}

func TestUnsubscribe_Success(t *testing.T) {
	db := getCleanDB(t)
	router, ghServer, _ := setupTestEnv(t, db)
	defer ghServer.Close()

	repo := infraDB.Repository{Owner: "testowner", Name: "testrepo", GitHubID: 12345}
	db.Create(&repo)

	sub := infraDB.Subscription{
		Email:        "user@example.com",
		RepositoryID: repo.ID,
		Status:       infraDB.StatusActive,
		ConfirmToken: "11111222223333344444555556666677",
		APIToken:     "12345678901234567890123456789012",
	}
	db.Create(&sub)

	w := performRequest(router, http.MethodGet, "/unsubscribe/"+sub.ConfirmToken, nil, map[string]string{
		"Authorization": "Bearer " + sub.APIToken,
	})
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d. Body: %s", w.Code, w.Body.String())
	}

	var updatedSub infraDB.Subscription
	db.First(&updatedSub, sub.ID)
	if updatedSub.Status != infraDB.StatusUnsubscribed {
		t.Fatalf("expected status %s, got %s", infraDB.StatusUnsubscribed, updatedSub.Status)
	}
}

func TestSubscribe_InvalidInputs(t *testing.T) {
	db := getCleanDB(t)
	router, ghServer, _ := setupTestEnv(t, db)
	defer ghServer.Close()

	tests := []struct {
		name     string
		payload  SubscribeRequest
		wantCode int
	}{
		{"Missing Email", SubscribeRequest{Repo: "testowner/testrepo"}, http.StatusBadRequest},
		{"Missing Repo", SubscribeRequest{Email: "user@example.com"}, http.StatusBadRequest},
		{"Invalid Repo Format", SubscribeRequest{Email: "user@example.com", Repo: "just_a_name"}, http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			body, _ := json.Marshal(tt.payload)
			w := performRequest(router, http.MethodPost, "/subscribe", body, map[string]string{"Content-Type": "application/json"})

			if w.Code != tt.wantCode {
				t.Errorf("expected %d, got %d", tt.wantCode, w.Code)
			}
		})
	}
}

func TestSubscribe_AlreadySubscribed(t *testing.T) {
	db := getCleanDB(t)
	router, ghServer, _ := setupTestEnv(t, db)
	defer ghServer.Close()

	// Seed DB with an active subscription
	repo := infraDB.Repository{Owner: "testowner", Name: "testrepo", GitHubID: 12345}
	db.Create(&repo)
	db.Create(&infraDB.Subscription{
		Email:        "user@example.com",
		RepositoryID: repo.ID,
		Status:       infraDB.StatusActive,
	})

	body, _ := json.Marshal(SubscribeRequest{Email: "user@example.com", Repo: "testowner/testrepo"})
	w := performRequest(router, http.MethodPost, "/subscribe", body, map[string]string{"Content-Type": "application/json"})

	if w.Code != http.StatusConflict {
		t.Errorf("expected 409 Conflict, got %d", w.Code)
	}
}

func TestSubscribe_EmailSenderError(t *testing.T) {
	db := getCleanDB(t)
	router, ghServer, emailSender := setupTestEnv(t, db)
	defer ghServer.Close()

	emailSender.err = errors.New("queue error")

	body, _ := json.Marshal(SubscribeRequest{Email: "user@example.com", Repo: "testowner/testrepo"})
	w := performRequest(router, http.MethodPost, "/subscribe", body, map[string]string{"Content-Type": "application/json"})

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected 500 Internal Server Error, got %d", w.Code)
	}
}

func TestConfirm_InvalidOrMissingToken(t *testing.T) {
	db := getCleanDB(t)
	router, ghServer, _ := setupTestEnv(t, db)
	defer ghServer.Close()

	w := performRequest(router, http.MethodGet, "/confirm/short-token", nil, nil)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request for short token, got %d", w.Code)
	}
}

func TestConfirm_TokenNotFound(t *testing.T) {
	db := getCleanDB(t)
	router, ghServer, _ := setupTestEnv(t, db)
	defer ghServer.Close()

	validLengthFakeToken := "12345678901234567890123456789012"
	w := performRequest(router, http.MethodGet, "/confirm/"+validLengthFakeToken, nil, nil)

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404 Not Found, got %d", w.Code)
	}
}

func TestConfirm_Idempotency(t *testing.T) {
	db := getCleanDB(t)
	router, ghServer, _ := setupTestEnv(t, db)
	defer ghServer.Close()

	// Seed DB with a pending subscription
	repo := infraDB.Repository{Owner: "testowner", Name: "testrepo", GitHubID: 12345}
	db.Create(&repo)
	confirmToken := "11111222223333344444555556666677"
	sub := infraDB.Subscription{
		Email:        "user@example.com",
		RepositoryID: repo.ID,
		Status:       infraDB.StatusPending,
		ConfirmToken: confirmToken,
	}
	db.Create(&sub)

	// First call to confirm
	w1 := performRequest(router, http.MethodGet, "/confirm/"+sub.ConfirmToken, nil, nil)
	if w1.Code != http.StatusOK {
		t.Fatalf("first confirm: expected 200 OK, got %d", w1.Code)
	}
	apiToken1 := w1.Header().Get("X-Api-Token")
	if apiToken1 == "" {
		t.Fatal("first confirm: expected an API token, got none")
	}

	// Second call to confirm should be idempotent
	w2 := performRequest(router, http.MethodGet, "/confirm/"+sub.ConfirmToken, nil, nil)
	if w2.Code != http.StatusOK {
		t.Fatalf("second confirm: expected 200 OK, got %d", w2.Code)
	}
	apiToken2 := w2.Header().Get("X-Api-Token")

	if apiToken1 != apiToken2 {
		t.Errorf("expected same API token on second call, got %s, want %s", apiToken2, apiToken1)
	}
}

func TestUnsubscribe_MissingAuth(t *testing.T) {
	db := getCleanDB(t)
	router, ghServer, _ := setupTestEnv(t, db)
	defer ghServer.Close()

	validLengthFakeToken := "12345678901234567890123456789012"
	w := performRequest(router, http.MethodGet, "/unsubscribe/"+validLengthFakeToken, nil, nil)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request for missing auth, got %d", w.Code)
	}
}

func TestUnsubscribe_WrongApiToken(t *testing.T) {
	db := getCleanDB(t)
	router, ghServer, _ := setupTestEnv(t, db)
	defer ghServer.Close()

	confirmToken := "aaaaabbbbbcccccdddddeeeeefffff11"
	realApiToken := "11111222223333344444555556666677"

	db.Create(&infraDB.Subscription{
		Email:        "user@example.com",
		Status:       infraDB.StatusActive,
		ConfirmToken: confirmToken,
		APIToken:     realApiToken,
	})

	wrongApiToken := "99999888887777766666555554444433"
	w := performRequest(router, http.MethodGet, "/unsubscribe/"+confirmToken, nil, map[string]string{
		"Authorization": "Bearer " + wrongApiToken,
	})

	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404 Not Found due to mismatched token, got %d", w.Code)
	}
}

func TestUnsubscribe_Idempotency(t *testing.T) {
	db := getCleanDB(t)
	router, ghServer, _ := setupTestEnv(t, db)
	defer ghServer.Close()

	repo := infraDB.Repository{Owner: "testowner", Name: "testrepo", GitHubID: 12345}
	db.Create(&repo)

	confirmToken := "aaaaabbbbbcccccdddddeeeeefffff11"
	apiToken := "11111222223333344444555556666677"

	db.Create(&infraDB.Subscription{
		Email:        "user@example.com",
		RepositoryID: repo.ID,
		Status:       infraDB.StatusActive,
		ConfirmToken: confirmToken,
		APIToken:     apiToken,
	})

	authHeader := map[string]string{"Authorization": "Bearer " + apiToken}

	// First call to unsubscribe
	w1 := performRequest(router, http.MethodGet, "/unsubscribe/"+confirmToken, nil, authHeader)
	if w1.Code != http.StatusOK {
		t.Fatalf("first unsubscribe: expected 200 OK, got %d", w1.Code)
	}

	// Second call to unsubscribe should be idempotent
	w2 := performRequest(router, http.MethodGet, "/unsubscribe/"+confirmToken, nil, authHeader)
	if w2.Code != http.StatusOK {
		t.Fatalf("second unsubscribe: expected 200 OK, got %d", w2.Code)
	}

	var finalSub infraDB.Subscription
	db.First(&finalSub, "confirm_token = ?", confirmToken)
	if finalSub.Status != infraDB.StatusUnsubscribed {
		t.Errorf("expected status to remain unsubscribed, got %s", finalSub.Status)
	}
}

func TestGetSubscriptions_MissingEmailParam(t *testing.T) {
	db := getCleanDB(t)
	router, ghServer, _ := setupTestEnv(t, db)
	defer ghServer.Close()

	validApiToken := "11111222223333344444555556666677"
	w := performRequest(router, http.MethodGet, "/subscriptions", nil, map[string]string{
		"Authorization": "Bearer " + validApiToken,
	})

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400 Bad Request for missing email, got %d", w.Code)
	}
}

func TestGetSubscriptions_Unauthorized(t *testing.T) {
	db := getCleanDB(t)
	router, ghServer, _ := setupTestEnv(t, db)
	defer ghServer.Close()

	db.Create(&infraDB.Subscription{
		Email:    "user@example.com",
		Status:   infraDB.StatusActive,
		APIToken: "11111222223333344444555556666677",
	})

	w := performRequest(router, http.MethodGet, "/subscriptions?email=user@example.com", nil, map[string]string{
		"Authorization": "Bearer 99999888887777766666555554444433",
	})

	if w.Code != http.StatusUnauthorized {
		t.Errorf("expected 401 Unauthorized, got %d", w.Code)
	}
}
