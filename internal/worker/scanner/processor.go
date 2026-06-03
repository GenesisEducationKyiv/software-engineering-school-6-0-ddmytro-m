package scanner

import (
	"context"
	"log"

	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/api/github"
	"github.com/GenesisEducationKyiv/software-engineering-school-6-0-ddmytro-m/internal/infra/db"
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
	defer func() {
		if err := p.store.UpdateScanStatus(repo); err != nil {
			log.Printf("error updating scan status for %s/%s: %v", repo.Owner, repo.Name, err)
		}
	}()

	repoResp := p.gh.GetRepository(ctx, repo.Owner, repo.Name, repo.ETag)

	switch repoResp.StatusCode {
	case 200:
		if repoResp.Data.ID != repo.GitHubID {
			log.Printf("repo ID mismatch for %s/%s: stored %d, got %d — skipping", repo.Owner, repo.Name, repo.GitHubID, repoResp.Data.ID)
			p.handleRepoMoved(repo)
			return
		}
		repo.ETag = repoResp.ETag

	case 304:
		// identity confirmed via ETag, proceed

	case 404:
		log.Printf("repo %s/%s no longer exists — skipping", repo.Owner, repo.Name)
		return

	case 403, 429:
		p.quota.Freeze()
		log.Printf("critical limit hit on repo check (%d). limiter frozen.", repoResp.StatusCode)
		return

	default:
		if repoResp.Error != nil {
			log.Printf("error checking repo %s/%s: %v", repo.Owner, repo.Name, repoResp.Error)
		}
		return
	}

	releaseResp := p.gh.GetLatestRelease(ctx, repo.Owner, repo.Name, repo.LastRelease.ETag)

	switch releaseResp.StatusCode {
	case 200:
		if releaseResp.Data.TagName != repo.LastRelease.TagName {
			log.Printf("new release for %s/%s: %s", repo.Owner, repo.Name, releaseResp.Data.TagName)
			p.handleNewRelease(repo, &releaseResp.Data)
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
		log.Printf("critical limit hit (%d). limiter frozen.", releaseResp.StatusCode)

	default:
		if releaseResp.Error != nil {
			log.Printf("error while getting latest release: %s", releaseResp.Error.Error())
		}
	}
}

func (p *domainRepoProcessor) handleNewRelease(repo *db.Repository, latestRelease *github.LatestRelease) {
	subs, err := p.store.GetActiveSubscriptions(repo.ID)
	if err != nil {
		log.Printf("error finding active subscriptions for %s/%s: %v", repo.Owner, repo.Name, err)
		return
	}

	for _, sub := range subs {
		if err := p.notifier.SendNewRelease(&sub, repo, latestRelease); err != nil {
			log.Printf("failed to notify %s for %s/%s: %v", sub.Email, repo.Owner, repo.Name, err)
		}
	}
}

func (p *domainRepoProcessor) handleRepoMoved(repo *db.Repository) {
	subs, err := p.store.GetActiveSubscriptions(repo.ID)
	if err != nil {
		log.Printf("error finding active subscriptions for %s/%s: %v", repo.Owner, repo.Name, err)
		return
	}

	for _, sub := range subs {
		if err := p.notifier.SendRepoMoved(&sub, repo); err != nil {
			log.Printf("failed to notify %s for %s/%s: %v", sub.Email, repo.Owner, repo.Name, err)
		}
	}

	if err := p.store.MarkMovedAndUnsubscribe(repo); err != nil {
		log.Printf("failed to handle db updates for moved repo %s/%s: %v", repo.Owner, repo.Name, err)
	}
}
