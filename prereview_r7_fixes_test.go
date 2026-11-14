// Copy: sdk; package: whatsmeow. Synthetic Noise peer and SQLite only.
package whatsmeow

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
)

func TestPrereviewR7DisconnectActuallyStopsStoreWork(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cli, _, _, slot := owedLabels(t, ctx, 102)
	original := cli.Store.AppState.(*sqlstore.SQLStore)
	counting := &prereviewClosedStore{AppStateStore: original}
	counting.failFirst.Store(true)
	cli.Store.AppState = counting
	cli.finishAppStateBatch(slot, nil)
	if err := cli.WaitForAppStateEvents(ctx); err != nil {
		t.Fatal(err)
	}
	cli.Disconnect()
	if err := cli.WaitForAppStateEvents(ctx); err != nil {
		t.Fatal(err)
	}
	before := counting.attempts.Load()
	if err := original.Container.Close(); err != nil {
		t.Fatal(err)
	}
	// Independent of notices: let the real former five-second timer mature.
	time.Sleep(5200 * time.Millisecond)
	debt, owed := cli.appStatePublisher.debt(appstate.WAPatchRegular)
	t.Logf("store_attempts_before=%d after=%d closed_store_accesses=%d timer=%t offline=%t owed=%t",
		before, counting.attempts.Load(), counting.closedErrors.Load(), debt.timer != nil, debt.offline, owed)
	if before != counting.attempts.Load() || counting.closedErrors.Load() != 0 || debt.timer != nil || !debt.offline {
		t.Fatal("disconnect did not actually park the worker")
	}
}

func TestPrereviewR7DeferredRecoveryCannotRegressNewerAdoption(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cli, _ := reviewLabelClient(t)
	cli.AppStateBacklogLimit = 1
	release, fetched, seen := holdLabelFetch(t, ctx, cli)
	cli.handleAppStateRecovery(ctx, "deferred-old", labelRecovery(t, cli, 110, "old-snapshot"))
	// A newer response gets a slot while the older refused response waits.
	cli.AppStateBacklogLimit = 2
	cli.handleAppStateRecovery(ctx, "newer", labelRecovery(t, cli, 120, "newest"))
	release()
	if err := <-fetched; err != nil {
		t.Fatal(err)
	}
	if err := cli.WaitForAppStateEvents(ctx); err != nil {
		t.Fatal(err)
	}
	delivered := seen.get()
	t.Logf("cursor=%d labels=%v", labelVersion(t, cli), delivered)
	if labelVersion(t, cli) != 120 || len(delivered) != 2 || delivered[1] != "newest" {
		t.Fatal("deferred older recovery regressed the newer committed state")
	}
}

type prereviewR7GatedFailure struct {
	store.AppStateStore
	entered, release chan struct{}
	attempts         atomic.Int32
}

func (s *prereviewR7GatedFailure) RecoverAppStateBackup(context.Context, string) error {
	s.attempts.Add(1)
	close(s.entered)
	<-s.release
	return errors.New("synthetic in-flight store error")
}

func TestPrereviewR7InflightFailureDoesNotRearmDisconnectedTimer(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cli, _, _, slot := owedLabels(t, ctx, 102)
	s := &prereviewR7GatedFailure{AppStateStore: cli.Store.AppState, entered: make(chan struct{}), release: make(chan struct{})}
	cli.Store.AppState = s
	cli.finishAppStateBatch(slot, nil)
	<-s.entered
	cli.Disconnect()
	close(s.release)
	if err := cli.WaitForAppStateEvents(ctx); err != nil {
		t.Fatal(err)
	}
	debt, _ := cli.appStatePublisher.debt(appstate.WAPatchRegular)
	t.Logf("disconnected=true in_flight_attempts=%d offline=%t timer_rearmed=%t", s.attempts.Load(), debt.offline, debt.timer != nil)
	defer func() {
		p := &cli.appStatePublisher
		p.mu.Lock()
		if p.owed[appstate.WAPatchRegular] != nil {
			p.owed[appstate.WAPatchRegular].stopTimer()
		}
		p.mu.Unlock()
	}()
	if debt.timer != nil {
		t.Fatal("an in-flight error rearmed a timer after Disconnect parked the debt")
	}
}
