package healthcheck

import (
	"context"
	"net/http"
	"strings"
	"time"

	"gateway/internal/db"
	"gateway/internal/models"
	"gateway/internal/notify"
)

type HealthChecker struct {
	db            *db.DB
	notifyService *notify.NotificationService
	interval      time.Duration
}

func New(database *db.DB, notifyService *notify.NotificationService, interval time.Duration) *HealthChecker {
	return &HealthChecker{
		db:            database,
		notifyService: notifyService,
		interval:      interval,
	}
}

func (hc *HealthChecker) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(hc.interval)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				hc.checkAll()
			}
		}
	}()
}

func (hc *HealthChecker) checkAll() {
	platforms, err := hc.db.GetPlatforms()
	if err != nil {
		return
	}

	for _, platform := range platforms {
		if !platform.Enabled {
			continue
		}

		rapis, err := hc.db.GetRAPIsByPlatform(platform.ID)
		if err != nil {
			continue
		}

		for _, rapi := range rapis {
			if !rapi.Enabled {
				continue
			}
			hc.checkRAPI(platform, rapi)
		}
	}
}

func (hc *HealthChecker) checkRAPI(platform models.Platform, rapi models.RAPIWithPlatform) {
	effectiveURL := models.NormalizeToCompletionsURL(platform.BaseURL)

	testBody := `{"model":"` + rapi.Model + `","messages":[{"role":"user","content":"hi"}],"max_tokens":1}`

	testClient := &http.Client{Timeout: 10 * time.Second}
	req, err := http.NewRequest("POST", effectiveURL, strings.NewReader(testBody))
	if err != nil {
		hc.markUnavailable(rapi)
		return
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+platform.Token)

	resp, err := testClient.Do(req)
	if err != nil {
		hc.markUnavailable(rapi)
		return
	}
	resp.Body.Close()

	if resp.StatusCode == http.StatusUnauthorized {
		hc.markAvailable(rapi)
		return
	}

	if resp.StatusCode >= 200 && resp.StatusCode < 500 {
		hc.markAvailable(rapi)
	} else {
		hc.markUnavailable(rapi)
	}
}

func (hc *HealthChecker) markAvailable(rapi models.RAPIWithPlatform) {
	if !rapi.Available {
		hc.db.SetRAPIAvailable(rapi.ID, true)
		hc.notifyService.PublishAsync("RAPI "+rapi.Alias+" recovered", "Health Check")
	}
}

func (hc *HealthChecker) markUnavailable(rapi models.RAPIWithPlatform) {
	if rapi.Available {
		hc.db.SetRAPIAvailable(rapi.ID, false)
		hc.notifyService.PublishAsync("RAPI "+rapi.Alias+" marked unhealthy", "Health Check")
	}
}
