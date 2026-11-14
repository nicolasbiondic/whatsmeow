// Copy: sdk; package: whatsmeow. Synthetic stores/peers only.
package whatsmeow

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
)

type prereviewClosedStore struct {
	store.AppStateStore
	attempts     atomic.Int32
	closedErrors atomic.Int32
	failFirst    atomic.Bool
}

func (s *prereviewClosedStore) RecoverAppStateBackup(ctx context.Context, name string) error {
	s.attempts.Add(1)
	if s.failFirst.Swap(false) {
		return errors.New("injected transient store failure before close")
	}
	err := s.AppStateStore.RecoverAppStateBackup(ctx, name)
	if err != nil && strings.Contains(err.Error(), "database is closed") {
		s.closedErrors.Add(1)
	}
	return err
}

func TestPrereviewR6NoDebtRetryAfterDisconnectAndStoreClose(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 12*time.Second)
	defer cancel()
	cli, _, notices, slot := owedLabels(t, ctx, 102)
	original := cli.Store.AppState.(*sqlstore.SQLStore)
	counting := &prereviewClosedStore{AppStateStore: original}
	counting.failFirst.Store(true)
	cli.Store.AppState = counting
	// Stop outstanding timers before synthetic test stores are cleaned up.
	defer func() {
		p := &cli.appStatePublisher
		p.mu.Lock()
		if debt := p.owed[appstate.WAPatchRegular]; debt != nil && debt.timer != nil {
			debt.timer.Stop()
		}
		delete(p.owed, appstate.WAPatchRegular)
		p.mu.Unlock()
	}()
	cli.finishAppStateBatch(slot, nil)
	if err := cli.WaitForAppStateEvents(ctx); err != nil {
		t.Fatal(err)
	}
	// A real failed attempt left its timer pending. Mirror App.Close:
	// disconnect -> barrier -> close store. The barrier excludes timers.
	cli.Disconnect()
	if err := cli.WaitForAppStateEvents(ctx); err != nil {
		t.Fatal(err)
	}
	if err := original.Container.Close(); err != nil {
		t.Fatal(err)
	}
	eventually(t, ctx, "the production five-second retry to finish", func() bool {
		return len(notices.failed()) >= 2
	})
	if err := cli.WaitForAppStateEvents(ctx); err != nil {
		t.Fatal(err)
	}
	debt, owed := cli.appStatePublisher.debt(appstate.WAPatchRegular)
	t.Logf("disconnected=true closed_store_attempts=%d failures=%d next_retry=%v offline=%t owed=%t",
		counting.closedErrors.Load(), len(notices.failed()), notices.failed()[1].RetryIn, debt.offline, owed)
	if counting.closedErrors.Load() > 0 && owed && !debt.offline && debt.timer != nil {
		t.Fatal("disconnected client retries the closed store and schedules more work after shutdown")
	}
}
