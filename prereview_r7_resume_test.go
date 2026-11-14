// Copy: sdk; package: whatsmeow. Synthetic Noise peer and SQLite only.
package whatsmeow

import (
	"context"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/appstate"
)

func TestPrereviewR7ResumePaysNewPausedNotificationAfterGiveUp(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cli, server, _, slot := owedLabels(t, ctx, 102)
	failing := &failingVersionStore{AppStateStore: cli.Store.AppState}
	failing.failures.Store(100)
	cli.Store.AppState = failing
	cli.appStatePublisher.retryBase = time.Millisecond
	cli.finishAppStateBatch(slot, nil)
	eventually(t, ctx, "five failures to park the debt", func() bool {
		debt, _ := cli.appStatePublisher.debt(appstate.WAPatchRegular)
		return debt.offline
	})
	if _, err := cli.PauseAppState(ctx); err != nil {
		t.Fatal(err)
	}
	failing.failures.Store(0)
	before := server.fetches.Load()
	// A new, consumed notification while paused must be paid on resume. It
	// is not the failed old attempt being retried without a new reason.
	cli.handleAppStateNotification(ctx, labelNotification(102))
	cli.ResumeAppState()
	if err := cli.WaitForAppStateEvents(ctx); err != nil {
		t.Fatal(err)
	}
	after := labelVersion(t, cli)
	debt, owed := cli.appStatePublisher.debt(appstate.WAPatchRegular)
	automatic := server.fetches.Load() - before
	if err := cli.FetchAppState(ctx, appstate.WAPatchRegular, false, false); err != nil {
		t.Fatal(err)
	}
	manual := labelVersion(t, cli)
	t.Logf("connected=%t new_notification_consumed=true after_resume=%d automatic_fetches=%d parked=%t owed=%t explicit_fetch=%d",
		cli.IsConnected(), after, automatic, debt.offline, owed, manual)
	if automatic == 0 && after != manual && debt.offline && owed {
		t.Fatal("Resume leaves the new paused notification parked on the previous failed attempt")
	}
}
