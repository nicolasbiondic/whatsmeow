// Copy: sdk; package: whatsmeow. Real incremental fetches over the synthetic
// Noise peer, with a consistently invalid regular-collection LTHash.
package whatsmeow

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/types/events"
)

func TestPrereviewR6BrokenCatchUpRetainsDebtAndEmitsErrors(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cli, server, notices, slot := owedLabels(t, ctx, 102)
	key, err := cli.Store.AppStateKeys.GetLatestAppStateSyncKeyID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// The local hash is deliberately omitted, so this valid-MAC patch has
	// a mismatching collection LTHash against the real stored base.
	raw, err := cli.appStateProc.EncodePatch(ctx, key, appstate.HashState{Version: 101}, appstate.BuildLabelEdit("8", "bad-base", 1, false))
	if err != nil {
		t.Fatal(err)
	}
	server.mu.Lock()
	server.version = 101
	server.appendPatch(raw)
	server.mu.Unlock()
	var mismatches atomic.Int32
	cli.AddEventHandler(func(evt any) {
		if v, ok := evt.(*events.AppStateSyncError); ok && errors.Is(v.Error, appstate.ErrMismatchingLTHash) {
			mismatches.Add(1)
		}
	})
	// Only test time is shortened; production defaults are exercised by the
	// closed-store probe. Retrying does not alter the error classification.
	cli.appStatePublisher.retryBase = 20 * time.Millisecond
	defer func() {
		cli.settleAppStateCatchUp(appstate.WAPatchRegular, cli.appStateCatchUpMark())
		_ = cli.WaitForAppStateEvents(ctx)
	}()
	cli.finishAppStateBatch(slot, nil)
	eventually(t, ctx, "two mismatch attempts", func() bool { return len(notices.failed()) >= 2 })
	if err := cli.WaitForAppStateEvents(ctx); err != nil {
		t.Fatal(err)
	}
	debt, owed := cli.appStatePublisher.debt(appstate.WAPatchRegular)
	t.Logf("incremental_mismatches=%d failed_notices=%d debt_retained=%t failures=%d timer_pending=%t", mismatches.Load(), len(notices.failed()), owed, debt.failures, debt.timer != nil)
	if mismatches.Load() < 2 || !owed || debt.timer == nil || len(notices.caughtUp()) != 0 {
		t.Fatal("unexpected broken-collection catch-up behavior")
	}
}
