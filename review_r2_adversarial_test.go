package whatsmeow

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"
)

func TestReviewR2PublisherBacklogAndCancellation(t *testing.T) {
	cli := &Client{Log: waLog.Noop}
	name := appstate.WAPatchRegular
	var delivered atomic.Int32
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	defer func() {
		close(release)
		<-done
		if delivered.Load() != 4096 || len(cli.appStatePublisher.pending) != 0 || len(cli.appStatePublisher.draining) != 0 {
			t.Fatal("released backlog did not drain and release queue bookkeeping")
		}
		t.Log("released backlog drained; no pending collection or drainer retained")
	}()
	go func() {
		defer close(done)
		cli.finishAppStateBatch(&appStateBatch{name: name}, func() {
			close(entered)
			<-release
		})
	}()
	<-entered
	ctx, cancel := context.WithCancel(t.Context())
	const count = 4096
	for range count {
		_, batch := newAppStateBatch(ctx, name)
		cli.appStatePublisher.reserve(batch)
		cli.finishAppStateBatch(batch, func() { delivered.Add(1) })
	}
	cancel()
	cli.appStatePublisher.mu.Lock()
	queued := len(cli.appStatePublisher.pending[name])
	cli.appStatePublisher.mu.Unlock()
	t.Logf("queued_behind_one_handler=%d delivered=%d context_canceled=%t", queued, delivered.Load(), ctx.Err() != nil)
	if queued != count || delivered.Load() != 0 {
		t.Fatal("unexpected publication behavior")
	}
}

// The only fixture change is allowing a synthetic full fetch to return a
// complete patch stream from zero instead of a snapshot. Production files and
// the original reviewer probe are unchanged.
func TestReviewR2SuccessfulRetryRetiresOldResetBackup(t *testing.T) {
	cli, server := reviewLabelClient(t)
	original := cli.Store.AppState.(*sqlstore.SQLStore)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	injected := errors.New("injected restore unavailable")
	cli.Store.AppState = &failingAtomicRestoreStore{
		cancelAfterResetStore: &cancelAfterResetStore{AppStateStore: original, cancel: cancel},
		failure: injected,
	}
	err := cli.FetchAppState(ctx, appstate.WAPatchRegular, true, false)
	if !errors.Is(err, context.Canceled) || !errors.Is(err, injected) {
		t.Fatal("failed to establish failed-cleanup boundary")
	}
	cli.Store.AppState = original
	version, _, err := original.GetAppStateVersion(t.Context(), "regular")
	if err != nil || version != 0 {
		t.Fatal("expected empty live state and a persistent old backup")
	}
	keyID, err := cli.Store.AppStateKeys.GetLatestAppStateSyncKeyID(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	raw, err := cli.appStateProc.EncodePatch(t.Context(), keyID, appstate.HashState{}, appstate.BuildLabelEdit("7", "retry", 1, false))
	if err != nil {
		t.Fatal(err)
	}
	server.mu.Lock()
	server.version = 0
	server.patches = make(map[uint64][]byte)
	server.appendPatch(raw)
	server.reviewAllowSnapshot = true
	server.mu.Unlock()
	if err := cli.FetchAppState(t.Context(), appstate.WAPatchRegular, true, false); err != nil {
		t.Fatal(err)
	}
	before, _, err := original.GetAppStateVersion(t.Context(), "regular")
	if err != nil || before != 1 {
		t.Fatal("retry did not establish the lower complete state")
	}
	jid, _ := types.ParseJID(original.JID)
	reopened := sqlstore.NewSQLStore(original.Container, jid)
	after, _, err := reopened.GetAppStateVersion(t.Context(), "regular")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("successful_retry_version=%d reopened_version=%d", before, after)
	if after != before {
		t.Fatal("stale reset backup undid a successful full replay on reopen")
	}
}
