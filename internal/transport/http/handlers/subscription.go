// Package handlers provides HTTP request handlers for the application.
package handlers

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"

	"github.com/gin-gonic/gin"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/db"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/metrics"
)

// EmailSender defines the interface for sending emails.
type EmailSender interface {
	SendEmailVerification(email, token string) error
}

// SubscriptionHandler handles HTTP requests related to subscriptions.
type SubscriptionHandler struct {
	store       SubscriptionRepository
	resolver    RepoResolver
	emailSender EmailSender
}

// NewSubscriptionHandler creates a new instance of SubscriptionHandler.
func NewSubscriptionHandler(store SubscriptionRepository, resolver RepoResolver, emailSender EmailSender) *SubscriptionHandler {
	return &SubscriptionHandler{
		store:       store,
		resolver:    resolver,
		emailSender: emailSender,
	}
}

// RegisterRoutes registers the subscription routes with the given router group.
func (h *SubscriptionHandler) RegisterRoutes(r *gin.RouterGroup) {
	r.POST("/subscribe", h.Subscribe)
	r.GET("/confirm/:token", h.Confirm)
	r.GET("/unsubscribe/:token", h.Unsubscribe)
	r.GET("/subscriptions", h.GetSubscriptions)
}

func (h *SubscriptionHandler) resolveOrCreateRepo(ctx context.Context, owner, name string) (*db.Repository, int, error) {
	repo, err := h.store.FindRepoByPath(owner, name)
	if err == nil {
		return repo, 0, nil
	}
	if !errors.Is(err, db.ErrNotFound) {
		return nil, http.StatusInternalServerError, errors.New("database error")
	}

	ghRepo := h.resolver.GetRepository(ctx, owner, name, "")
	if ghRepo.Error != nil || ghRepo.StatusCode != 200 {
		return nil, http.StatusNotFound, errors.New("repository not found on GitHub")
	}

	existing, lookupErr := h.store.FindRepoByGitHubID(ghRepo.Data.ID)
	switch {
	case lookupErr == nil:
		existing.Owner = owner
		existing.Name = name
		if saveErr := h.store.SaveRepo(existing); saveErr != nil {
			return nil, http.StatusInternalServerError, errors.New("failed to save repository")
		}
		return existing, 0, nil
	case errors.Is(lookupErr, db.ErrNotFound):
		newRepo := &db.Repository{GitHubID: ghRepo.Data.ID, Owner: owner, Name: name, Status: db.StatusIdle}
		if createErr := h.store.CreateRepo(newRepo); createErr != nil {
			return nil, http.StatusInternalServerError, errors.New("failed to save repository")
		}
		return newRepo, 0, nil
	default:
		return nil, http.StatusInternalServerError, errors.New("database error")
	}
}

func generateToken() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}

func isValidToken(token string) bool {
	_, err := hex.DecodeString(token)
	return err == nil && len(token) == 32
}

func bearerToken(c *gin.Context) string {
	auth := c.GetHeader("Authorization")

	if !strings.HasPrefix(auth, "Bearer ") {
		return ""
	}

	return strings.TrimPrefix(c.GetHeader("Authorization"), "Bearer ")
}

// SubscribeRequest represents the JSON body of a subscribe request.
type SubscribeRequest struct {
	Email string `json:"email"`
	Repo  string `json:"repo"`
}

// Subscribe handles the creation or updating of a subscription.
func (h *SubscriptionHandler) Subscribe(c *gin.Context) {
	metrics.SubscribeAttempts.Inc()

	var req SubscribeRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request body"})
		return
	}

	req.Email = strings.TrimSpace(req.Email)
	req.Repo = strings.TrimSpace(req.Repo)

	if req.Email == "" || req.Repo == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Email and repo are required"})
		return
	}

	parts := strings.Split(req.Repo, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid repo format. Expected owner/repo"})
		return
	}
	owner, name := parts[0], parts[1]

	repo, httpStatus, resolveErr := h.resolveOrCreateRepo(c.Request.Context(), owner, name)
	if resolveErr != nil {
		c.JSON(httpStatus, gin.H{"error": resolveErr.Error()})
		return
	}

	sub, err := h.store.FindSubscription(req.Email, repo.ID)
	if err != nil && !errors.Is(err, db.ErrNotFound) {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	recordExists := err == nil

	if recordExists && sub.Status == db.StatusActive {
		metrics.SubscribeConflicts.Inc()
		c.JSON(http.StatusConflict, gin.H{"message": "Email already subscribed to this repository"})
		return
	}

	var confirmToken string
	for {
		confirmToken, err = generateToken()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to generate token"})
			return
		}
		taken, tokenErr := h.store.IsConfirmTokenTaken(confirmToken)
		if tokenErr != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to generate token"})
			return
		}
		if !taken {
			break
		}
	}

	if recordExists {
		// re-send confirmation for a pending or unsubscribed record.
		sub.Status = db.StatusPending
		sub.ConfirmToken = confirmToken
		sub.APIToken = "" // only issued on confirmation
		if err := h.store.SaveSubscription(sub); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update subscription"})
			return
		}
	} else {
		sub = &db.Subscription{
			Email:        req.Email,
			RepositoryID: repo.ID,
			Status:       db.StatusPending,
			ConfirmToken: confirmToken,
			APIToken:     "",
		}
		if err := h.store.CreateSubscription(sub); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create subscription"})
			return
		}
	}

	if h.emailSender != nil {
		if err := h.emailSender.SendEmailVerification(sub.Email, sub.ConfirmToken); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to queue verification email"})
			return
		}
	}

	metrics.SubscribeSuccesses.Inc()
	c.JSON(http.StatusOK, gin.H{"message": "Confirmation email sent"})
}

// Confirm handles the confirmation of a pending subscription.
func (h *SubscriptionHandler) Confirm(c *gin.Context) {
	metrics.ConfirmAttempts.Inc()

	token := c.Param("token")
	if !isValidToken(token) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid or missing token"})
		return
	}

	sub, err := h.store.FindSubscriptionByConfirmToken(token)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "Invalid or expired token"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	if sub.Status != db.StatusActive {
		apiToken, err := generateToken()
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to generate API token"})
			return
		}
		sub.Status = db.StatusActive
		sub.APIToken = apiToken
		if err := h.store.SaveSubscription(sub); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to activate subscription"})
			return
		}
	}

	metrics.ConfirmSuccesses.Inc()
	c.Header("X-Api-Token", sub.APIToken)
	c.JSON(http.StatusOK, gin.H{"message": "Subscription confirmed successfully"})
}

// Unsubscribe handles the removal of an active subscription.
func (h *SubscriptionHandler) Unsubscribe(c *gin.Context) {
	metrics.UnsubscribeAttempts.Inc()

	confirmToken := c.Param("token")
	if !isValidToken(confirmToken) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid or missing token"})
		return
	}

	apiToken := bearerToken(c)
	if apiToken == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Missing or invalid Authorization header"})
		return
	}
	if !isValidToken(apiToken) {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid API token"})
		return
	}

	sub, err := h.store.FindSubscriptionByTokens(confirmToken, apiToken)
	if err != nil {
		if errors.Is(err, db.ErrNotFound) {
			c.JSON(http.StatusNotFound, gin.H{"error": "Token not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	if sub.Status != db.StatusUnsubscribed {
		sub.Status = db.StatusUnsubscribed
		if err := h.store.SaveSubscription(sub); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to unsubscribe"})
			return
		}
	}

	metrics.UnsubscribeSuccesses.Inc()
	c.JSON(http.StatusOK, gin.H{"message": "Unsubscribed successfully"})
}

// SubscriptionItem represents a single subscription in the response list.
type SubscriptionItem struct {
	Email       string `json:"email"`
	Repo        string `json:"repo"`
	Confirmed   bool   `json:"confirmed"`
	LastSeenTag string `json:"last_seen_tag"`
}

// GetSubscriptions handles fetching all subscriptions for a specific user.
func (h *SubscriptionHandler) GetSubscriptions(c *gin.Context) {
	email := strings.TrimSpace(c.Query("email"))
	if email == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid email"})
		return
	}

	apiToken := bearerToken(c)
	if apiToken == "" || !isValidToken(apiToken) {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Missing or invalid Authorization header"})
		return
	}

	if _, err := h.store.FindSubscriptionByEmailAndToken(email, apiToken); err != nil {
		if errors.Is(err, db.ErrNotFound) {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid token for the given email"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	subs, err := h.store.ListSubscriptions(email)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	if len(subs) == 0 {
		c.JSON(http.StatusOK, []SubscriptionItem{})
		return
	}

	repoIDs := make([]uint, 0, len(subs))
	for _, s := range subs {
		repoIDs = append(repoIDs, s.RepositoryID)
	}

	repos, err := h.store.FindReposByIDs(repoIDs)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	type repoInfo struct {
		FullName    string
		LastSeenTag string
	}
	repoMap := make(map[uint]repoInfo, len(repos))
	for _, repo := range repos {
		repoMap[repo.ID] = repoInfo{
			FullName:    repo.Owner + "/" + repo.Name,
			LastSeenTag: repo.LastRelease.TagName,
		}
	}

	items := make([]SubscriptionItem, 0, len(subs))
	for _, s := range subs {
		if info, ok := repoMap[s.RepositoryID]; ok {
			lastSeenTag := ""
			if s.Status == db.StatusActive {
				lastSeenTag = info.LastSeenTag
			}

			items = append(items, SubscriptionItem{
				Email:       s.Email,
				Repo:        info.FullName,
				Confirmed:   s.Status == db.StatusActive,
				LastSeenTag: lastSeenTag,
			})
		}
	}

	c.JSON(http.StatusOK, items)
}
