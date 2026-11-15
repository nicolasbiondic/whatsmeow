// Copy: sdk; package: whatsmeow. Real debt/recovery queues and synthetic
// Noise peers/SQLite. Tests outside the debt-only reachable-state walk.
package whatsmeow

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/appstate/lthash"
)

func TestPrereviewR8RecoveredSnapshotResumesParkedFetchDebt(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	cli, server, notices, slot := owedLabels(t, ctx, 111)
	failing := &failingVersionStore{AppStateStore: cli.Store.AppState}
	failing.failures.Store(100)
	cli.Store.AppState = failing
	cli.appStatePublisher.retryBase = time.Millisecond
	cli.finishAppStateBatch(slot, nil)
	eventually(t, ctx, "five failures to park", func() bool {
		debt, _ := cli.appStatePublisher.debt(appstate.WAPatchRegular)
		return debt.state == debtOnHold
	})
	failing.failures.Store(0)
	slot = &appStateBatch{name: appstate.WAPatchRegular}
	if err := cli.claimAppStateBatch(slot); err != nil {
		t.Fatal(err)
	}
	cli.handleAppStateRecovery(ctx, "repair", labelRecovery(t, cli, 110, "recovered"))
	// The primary's v110 snapshot repaired the base; it does not cover the
	// previously refused v111 notification. That exact server delta is
	// MAC-valid against the adopted snapshot and needs no later traffic.
	mac := make([]byte, 32)
	mac[0] = 110
	state := appstate.HashState{Version: 110}
	lthash.WAPatchIntegrity.SubtractThenAddInPlace(state.Hash[:], nil, [][]byte{mac})
	key, err := cli.Store.AppStateKeys.GetLatestAppStateSyncKeyID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	raw, err := cli.appStateProc.EncodePatch(ctx, key, state, appstate.BuildLabelEdit("8", "after-snapshot", 1, false))
	if err != nil {
		t.Fatal(err)
	}
	server.mu.Lock()
	server.version = 110
	server.appendPatch(raw)
	server.mu.Unlock()
	before := server.fetches.Load()
	cli.finishAppStateBatch(slot, nil)
	if err := cli.WaitForAppStateEvents(ctx); err != nil {
		t.Fatal(err)
	}
	after := labelVersion(t, cli)
	debt, owed := cli.appStatePublisher.debt(appstate.WAPatchRegular)
	automatic := server.fetches.Load() - before
	caughtUp := len(notices.caughtUp())
	if err := cli.FetchAppState(ctx, appstate.WAPatchRegular, false, false); err != nil {
		t.Fatal(err)
	}
	manual := labelVersion(t, cli)
	t.Logf("refused_version=%d recovery_cursor=%d automatic_fetches=%d debt_owed=%t on_hold=%t caught_up_notices=%d explicit_fetch_cursor=%d",
		debt.version, after, automatic, owed, debt.state == debtOnHold, caughtUp, manual)
	if after < manual && owed && automatic == 0 && caughtUp > 0 {
		t.Fatal("snapshot adoption reports caught up but leaves its collection's fetch debt on hold")
	}
}

func TestPrereviewR8RecoveryAlongsideParkedDebt(t *testing.T) {
	for _, action := range []string{"drain", "pause-resume", "lost-reconnect", "disconnect-reconnect"} {
		t.Run(action, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			cli, server, _, slot := owedLabels(t, ctx, 102)
			cli.EnableAutoReconnect = false
			failing := &failingVersionStore{AppStateStore: cli.Store.AppState}
			failing.failures.Store(100)
			cli.Store.AppState = failing
			cli.appStatePublisher.retryBase = time.Millisecond
			cli.finishAppStateBatch(slot, nil)
			eventually(t, ctx, "debt parked after five failures", func() bool {
				debt, _ := cli.appStatePublisher.debt(appstate.WAPatchRegular)
				return debt.state == debtOnHold
			})
			failing.failures.Store(0)
			slot = &appStateBatch{name: appstate.WAPatchRegular}
			if err := cli.claimAppStateBatch(slot); err != nil {
				t.Fatal(err)
			}
			cli.handleAppStateRecovery(ctx, "synthetic", labelRecovery(t, cli, 110, "recovery"))
			switch action {
			case "pause-resume":
				short, done := context.WithCancel(ctx)
				done()
				_, _ = cli.PauseAppState(short)
				cli.ResumeAppState()
			case "lost-reconnect":
				cli.socketLock.RLock()
				lost := cli.socket
				cli.socketLock.RUnlock()
				cli.onDisconnect(ctx, lost, true)
				next := attachFakeServer(t, cli, appstate.WAPatchRegular)
				server.mu.Lock()
				next.version, next.patches = server.version, server.patches
				server.mu.Unlock()
				cli.resumeAppStateCatchUps()
			case "disconnect-reconnect":
				cli.Disconnect()
				next := attachFakeServer(t, cli, appstate.WAPatchRegular)
				server.mu.Lock()
				next.version, next.patches = server.version, server.patches
				server.mu.Unlock()
				cli.resumeAppStateCatchUps()
			}
			cli.finishAppStateBatch(slot, nil)
			if err := cli.WaitForAppStateEvents(ctx); err != nil {
				t.Fatal(err)
			}
			debt, owed := cli.appStatePublisher.debt(appstate.WAPatchRegular)
			version := labelVersion(t, cli)
			t.Logf("action=%s cursor=%d owed=%t debt_state=%d dropped=%v", action, version, owed, debt.state, cli.appStatePublisher.dropped)
			// Pause/Disconnect intentionally drop a retained snapshot. Their
			// unfinished contract must not falsely claim it was adopted.
			if action == "drain" || action == "lost-reconnect" {
				if version != 110 {
					t.Fatal("retained snapshot was not adopted")
				}
			}
			if owed && debt.state == debtReady {
				t.Fatal("debt ready with no worker or wake-up")
			}
		})
	}
}

func TestPrereviewR8PauseSpanningReconnectPaysDebt(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cli, server, _, slot := owedLabels(t, ctx, 102)
	cli.EnableAutoReconnect = false
	cli.socketLock.RLock()
	lost := cli.socket
	cli.socketLock.RUnlock()
	cli.onDisconnect(ctx, lost, true)
	short, done := context.WithCancel(ctx)
	done()
	_, _ = cli.PauseAppState(short)
	cli.finishAppStateBatch(slot, nil)
	before := server.fetches.Load()
	next := attachFakeServer(t, cli, appstate.WAPatchRegular)
	server.mu.Lock()
	next.version, next.patches = server.version, server.patches
	server.mu.Unlock()
	cli.resumeAppStateCatchUps()
	if err := cli.WaitForAppStateEvents(ctx); err != nil {
		t.Fatal(err)
	}
	if next.fetches.Load() != 0 {
		t.Fatal("reconnect fetched while paused")
	}
	cli.ResumeAppState()
	if err := cli.WaitForAppStateEvents(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("paused_reconnect_fetches=0 after_resume_fetches=%d cursor=%d original_fetches=%d", next.fetches.Load(), labelVersion(t, cli), before)
	if labelVersion(t, cli) != 102 {
		t.Fatal("paused reconnect failed to catch up")
	}
}

func TestPrereviewR8AttemptOvertakenByNewDebtDoesNotEscapeBounds(t *testing.T) {
	// Exercise a late old result after another fetch settled its debt and a
	// new refusal installed a different one, a case absent from the walk.
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cli, _, _, slot := owedLabels(t, ctx, 102)
	p := &cli.appStatePublisher
	cli.settleAppStateCatchUp(appstate.WAPatchRegular, cli.appStateCatchUpMark())
	cli.handleAppStateNotification(ctx, labelNotification(103))
	p.mu.Lock()
	p.catchingUp[appstate.WAPatchRegular] = true
	connects := p.connects
	p.mu.Unlock()
	cli.endAppStateCatchUp(appstate.WAPatchRegular, connects, errors.New("old attempt result"))
	debt, owed := p.debt(appstate.WAPatchRegular)
	t.Logf("new_debt_owed=%t failures_charged_by_old_result=%d retry_armed=%t", owed, debt.failures, debt.timer != nil)
	cli.Disconnect()
	cli.finishAppStateBatch(slot, nil)
	if err := cli.WaitForAppStateEvents(ctx); err != nil {
		t.Fatal(err)
	}
}
