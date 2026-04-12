package handlers

import (
	"crypto/rand"
	"encoding/hex"
	"net/http"
	"strings"

	"github.com/ddmytro-m/github-scanner/internal/api/github"
	"github.com/ddmytro-m/github-scanner/internal/infra/db"
	"github.com/ddmytro-m/github-scanner/internal/infra/mq"
	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type SubscriptionHandler struct {
	db       *gorm.DB
	ghClient *github.GitHubClient
	emailMQ  *mq.EmailMQ
}

func NewSubscriptionHandler(db *gorm.DB, ghClient *github.GitHubClient, emailMQ *mq.EmailMQ) *SubscriptionHandler {
	return &SubscriptionHandler{
		db:       db,
		ghClient: ghClient,
		emailMQ:  emailMQ,
	}
}

func (h *SubscriptionHandler) RegisterRoutes(r *gin.RouterGroup) {
	r.POST("/subscribe", h.Subscribe)
	r.GET("/confirm/:token", h.Confirm)
	r.GET("/unsubscribe/:token", h.Unsubscribe)
	r.GET("/subscriptions", h.GetSubscriptions)
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

type SubscribeRequest struct {
	Email string `json:"email"`
	Repo  string `json:"repo"`
}

func (h *SubscriptionHandler) Subscribe(c *gin.Context) {
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

	// resolve or create the repository record.
	var repo db.Repository
	err := h.db.Where("owner = ? AND name = ?", owner, name).First(&repo).Error
	if err != nil {
		if err != gorm.ErrRecordNotFound {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
			return
		}

		ghRepo := h.ghClient.GetRepository(c.Request.Context(), owner, name, "")
		if ghRepo.Error != nil || ghRepo.StatusCode != 200 {
			c.JSON(http.StatusNotFound, gin.H{"error": "Repository not found on GitHub"})
			return
		}

		err = h.db.Where("github_id = ?", ghRepo.Data.ID).First(&repo).Error
		if err == nil {
			// repo already exists (renamed/removed)
			repo.Owner = owner
			repo.Name = name
			h.db.Save(&repo)
		} else if err == gorm.ErrRecordNotFound {
			repo = db.Repository{
				GitHubID: ghRepo.Data.ID,
				Owner:    owner,
				Name:     name,
				Status:   db.StatusIdle,
			}
			if err := h.db.Create(&repo).Error; err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to save repository"})
				return
			}
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
			return
		}
	}

	var sub db.Subscription
	err = h.db.Where("email = ? AND repository_id = ?", req.Email, repo.ID).First(&sub).Error
	if err != nil && err != gorm.ErrRecordNotFound {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	if err == nil {
		if sub.Status == db.StatusActive {
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
			var count int64
			h.db.Model(&db.Subscription{}).Where("confirm_token = ?", confirmToken).Count(&count)
			if count == 0 {
				break
			}
		}

		// re-send confirmation for a pending or unsubscribed record.
		sub.Status = db.StatusPending
		sub.ConfirmToken = confirmToken
		sub.ApiToken = "" // only issued on confirmation
		if err := h.db.Save(&sub).Error; err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update subscription"})
			return
		}
	} else {
		var confirmToken string
		for {
			confirmToken, err = generateToken()
			if err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to generate token"})
				return
			}
			var count int64
			h.db.Model(&db.Subscription{}).Where("confirm_token = ?", confirmToken).Count(&count)
			if count == 0 {
				break
			}
		}

		sub = db.Subscription{
			Email:        req.Email,
			RepositoryID: repo.ID,
			Status:       db.StatusPending,
			ConfirmToken: confirmToken,
			ApiToken:     "",
		}
		if err := h.db.Create(&sub).Error; err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create subscription"})
			return
		}
	}

	if h.emailMQ != nil {
		if err := h.emailMQ.SendEmailVerification(sub.Email, sub.ConfirmToken); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to queue verification email"})
			return
		}
	}

	c.JSON(http.StatusOK, gin.H{"message": "Confirmation email sent"})
}

func (h *SubscriptionHandler) Confirm(c *gin.Context) {
	token := c.Param("token")
	if !isValidToken(token) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid or missing token"})
		return
	}

	var sub db.Subscription
	if err := h.db.Where("confirm_token = ?", token).First(&sub).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
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
		sub.ApiToken = apiToken
		if err := h.db.Save(&sub).Error; err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to activate subscription"})
			return
		}
	}

	c.Header("X-Api-Token", sub.ApiToken)
	c.JSON(http.StatusOK, gin.H{"message": "Subscription confirmed successfully"})
}

func (h *SubscriptionHandler) Unsubscribe(c *gin.Context) {
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

	var sub db.Subscription
	if err := h.db.Where("confirm_token = ? AND api_token = ?", confirmToken, apiToken).First(&sub).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			c.JSON(http.StatusNotFound, gin.H{"error": "Token not found"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	if sub.Status != db.StatusUnsubscribed {
		sub.Status = db.StatusUnsubscribed
		if err := h.db.Save(&sub).Error; err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to unsubscribe"})
			return
		}
	}

	c.JSON(http.StatusOK, gin.H{"message": "Unsubscribed successfully"})
}

type SubscriptionItem struct {
	Email       string `json:"email"`
	Repo        string `json:"repo"`
	Confirmed   bool   `json:"confirmed"`
	LastSeenTag string `json:"last_seen_tag"`
}

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

	var caller db.Subscription
	if err := h.db.Where("email = ? AND api_token = ?", email, apiToken).First(&caller).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid token for the given email"})
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Database error"})
		return
	}

	var subs []db.Subscription
	if err := h.db.Where("email = ? AND status != ?", email, db.StatusUnsubscribed).Find(&subs).Error; err != nil {
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

	var repos []db.Repository
	if err := h.db.Where("id IN ?", repoIDs).Find(&repos).Error; err != nil {
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
