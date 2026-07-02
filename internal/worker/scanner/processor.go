package scanner

import (
	"context"

	"go.uber.org/zap"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/api/github"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/events"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/db"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/outbox"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/logger"
)

// RepoProcessor defines the contract for processing a single repository.
type RepoProcessor interface {
	ProcessRepo(ctx context.Context, repo *db.Repository)
}

type domainRepoProcessor struct {
	store    RepositoryStore
	gh       *github.Client
	notifier Notifier
	quota    QuotaManager
}

// NewRepoProcessor creates a new RepoProcessor.
func NewRepoProcessor(store RepositoryStore, gh *github.Client, notifier Notifier, quota QuotaManager) RepoProcessor {
	return &domainRepoProcessor{
		store:    store,
		gh:       gh,
		notifier: notifier,
		quota:    quota,
	}
}

func (p *domainRepoProcessor) ProcessRepo(ctx context.Context, repo *db.Repository) {
	var pending []outbox.Event
	defer func() {
		if err := p.store.UpdateScanStatus(repo, pending...); err != nil {
			logger.Log.Error("error updating scan status", zap.String("owner", repo.Owner), zap.String("name", repo.Name), zap.Error(err))
		}
	}()

	repoResp := p.gh.GetRepository(ctx, repo.Owner, repo.Name, repo.ETag)

	switch repoResp.StatusCode {
	case 200:
		if repoResp.Data.ID != repo.GitHubID {
			logger.Log.Warn("repo ID mismatch — skipping", zap.String("owner", repo.Owner), zap.String("name", repo.Name), zap.Int64("stored_id", repo.GitHubID), zap.Int64("got_id", repoResp.Data.ID))
			p.handleRepoMoved(repo)
			return
		}
		repo.ETag = repoResp.ETag

	case 304:
		// identity confirmed via ETag, proceed

	case 404:
		logger.Log.Warn("repo no longer exists — skipping", zap.String("owner", repo.Owner), zap.String("name", repo.Name))
		return

	case 403, 429:
		p.quota.Freeze()
		logger.Log.Error("critical limit hit on repo check, limiter frozen", zap.Int("status", repoResp.StatusCode))
		return

	default:
		if repoResp.Error != nil {
			logger.Log.Error("error checking repo", zap.String("owner", repo.Owner), zap.String("name", repo.Name), zap.Error(repoResp.Error))
		}
		return
	}

	releaseResp := p.gh.GetLatestRelease(ctx, repo.Owner, repo.Name, repo.LastRelease.ETag)

	switch releaseResp.StatusCode {
	case 200:
		if releaseResp.Data.TagName != repo.LastRelease.TagName {
			logger.Log.Info("new release detected", zap.String("owner", repo.Owner), zap.String("name", repo.Name), zap.String("tag", releaseResp.Data.TagName))
			pending = p.handleNewRelease(repo, &releaseResp.Data)
			repo.LastRelease.TagName = releaseResp.Data.TagName
			repo.LastRelease.GitHubID = releaseResp.Data.ID
		}
		repo.LastRelease.ETag = releaseResp.ETag

	case 304:
		// no change

	case 404:
		// repo have no latest release

	case 403, 429:
		p.quota.Freeze()
		logger.Log.Error("critical limit hit on release check, limiter frozen", zap.Int("status", releaseResp.StatusCode))

	default:
		if releaseResp.Error != nil {
			logger.Log.Error("error while getting latest release", zap.String("owner", repo.Owner), zap.String("name", repo.Name), zap.Error(releaseResp.Error))
		}
	}
}

// handleNewRelease builds one release.detected outbox event per active
// subscriber. The caller persists them atomically with the release state
// update so a notification is never lost to a broker outage.
func (p *domainRepoProcessor) handleNewRelease(repo *db.Repository, latestRelease *github.LatestRelease) []outbox.Event {
	subs, err := p.store.GetActiveSubscriptions(repo.ID)
	if err != nil {
		logger.Log.Error("error finding active subscriptions", zap.String("owner", repo.Owner), zap.String("name", repo.Name), zap.Error(err))
		return nil
	}

	outboxEvents := make([]outbox.Event, 0, len(subs))
	for _, sub := range subs {
		ev, err := outbox.New(events.NewReleaseDetected(events.ReleaseDetected{
			Email:      sub.Email,
			Repo:       repo.Owner + "/" + repo.Name,
			ReleaseTag: latestRelease.TagName,
		}))
		if err != nil {
			logger.Log.Error("failed to build release notification event", zap.String("owner", repo.Owner), zap.String("name", repo.Name), zap.Error(err))
			continue
		}
		outboxEvents = append(outboxEvents, ev)
	}
	return outboxEvents
}

func (p *domainRepoProcessor) handleRepoMoved(repo *db.Repository) {
	subs, err := p.store.GetActiveSubscriptions(repo.ID)
	if err != nil {
		logger.Log.Error("error finding active subscriptions", zap.String("owner", repo.Owner), zap.String("name", repo.Name), zap.Error(err))
		return
	}

	for _, sub := range subs {
		if err := p.notifier.SendRepoMoved(&sub, repo); err != nil {
			logger.Log.Error("failed to notify subscriber", zap.String("owner", repo.Owner), zap.String("name", repo.Name), zap.Error(err))
		}
	}

	if err := p.store.MarkMovedAndUnsubscribe(repo); err != nil {
		logger.Log.Error("failed to handle db updates for moved repo", zap.String("owner", repo.Owner), zap.String("name", repo.Name), zap.Error(err))
	}
}
