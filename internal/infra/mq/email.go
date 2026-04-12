package mq

import (
	"context"
	"sync"

	"github.com/ddmytro-m/github-scanner/internal/api/github"
	"github.com/ddmytro-m/github-scanner/internal/infra/db"
	redisDB "github.com/ddmytro-m/github-scanner/internal/infra/redis"

	"github.com/redis/go-redis/v9"
)

const DeliveryStream = "messages:delivery"

type EventType string

const (
	EventNewRelease        EventType = "new_release"
	EventRepoMoved         EventType = "repo_moved"
	EventEmailVerification EventType = "email_verification"
)

type DeliveryMessage struct {
	Event   EventType      `json:"event"`
	Email   string         `json:"email"`
	Repo    string         `json:"repo,omitempty"`
	Release string         `json:"release,omitempty"`
	Payload map[string]any `json:"payload,omitempty"`
}

type EmailMQ struct {
	stream *redisDB.RedisStream
}

var (
	instance *EmailMQ
	once     sync.Once
)

func GetEmailMQ(client *redis.Client) *EmailMQ {
	once.Do(func() {
		stream := redisDB.NewRedisStream(client, DeliveryStream)
		if stream == nil {
			return
		}

		instance = &EmailMQ{
			stream: stream,
		}
	})

	return instance
}

func (mq *EmailMQ) SendNewRelease(sub *db.Subscription, repo *db.Repository, release *github.LatestRelease) error {
	return mq.stream.Publish(context.Background(), DeliveryMessage{
		Event:   EventNewRelease,
		Email:   sub.Email,
		Repo:    repo.Owner + "/" + repo.Name,
		Release: release.TagName,
	})
}

func (mq *EmailMQ) SendRepoMoved(sub *db.Subscription, repo *db.Repository) error {
	return mq.stream.Publish(context.Background(), DeliveryMessage{
		Event: EventRepoMoved,
		Email: sub.Email,
		Repo:  repo.Owner + "/" + repo.Name,
	})
}

func (mq *EmailMQ) SendEmailVerification(email, token string) error {
	return mq.stream.Publish(context.Background(), DeliveryMessage{
		Event: EventEmailVerification,
		Email: email,
		Payload: map[string]any{
			"token": token,
		},
	})
}
