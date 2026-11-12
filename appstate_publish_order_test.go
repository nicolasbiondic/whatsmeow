package whatsmeow

import (
	"context"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// appendLabelPatch stores a label rename on the fake server, encoded on top
// of the collection state the client currently has.
func appendLabelPatch(t *testing.T, cli *Client, server *fakeSyncdServer, label string) {
	t.Helper()
	ctx := context.Background()
	keyID, err := cli.Store.AppStateKeys.GetLatestAppStateSyncKeyID(ctx)
	if err != nil {
		t.Fatal(err)
	}
	version, hash, err := cli.Store.AppState.GetAppStateVersion(ctx, string(appstate.WAPatchRegular))
	if err != nil {
		t.Fatal(err)
	}
	patch, err := cli.appStateProc.EncodePatch(ctx, keyID, appstate.HashState{Version: version, Hash: hash}, appstate.BuildLabelEdit("7", label, 1, false))
	if err != nil {
		t.Fatal(err)
	}
	server.mu.Lock()
	server.version = version
	server.mu.Unlock()
	server.appendPatch(patch)
}

type labelLog struct {
	mu    sync.Mutex
	names []string
}

func (l *labelLog) add(evts ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, evt := range evts {
		if label, ok := evt.(*events.LabelEdit); ok {
			l.names = append(l.names, label.Action.GetName())
		}
	}
}

func (l *labelLog) get() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.names...)
}

// A handler that fetches the collection whose batch it is being called for
// must neither deadlock nor see the nested batch before the current one ends.
func TestAppStateHandlerMayFetchItsOwnCollection(t *testing.T) {
	cli, server := reviewLabelClient(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	var seen labelLog
	var nested atomic.Bool
	nestedErr := make(chan error, 1)
	var duringNested []string
	cli.AddEventHandler(func(evt any) {
		label, ok := evt.(*events.LabelEdit)
		if !ok || label.Action.GetName() != "old" || nested.Swap(true) {
			return
		}
		appendLabelPatch(t, cli, server, "newer")
		nestedErr <- cli.FetchAppState(ctx, appstate.WAPatchRegular, false, false)
		duringNested = seen.get()
	})
	cli.AddEventHandler(func(evt any) { seen.add(evt) })
	done := make(chan error, 1)
	go func() { done <- cli.FetchAppState(ctx, appstate.WAPatchRegular, false, false) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("a handler fetching its own collection deadlocked the publication")
	}
	if err := <-nestedErr; err != nil {
		t.Fatalf("nested fetch: %v", err)
	}
	if len(duringNested) != 0 {
		t.Fatalf("nested batch was delivered inside the handler of the earlier batch: %v", duringNested)
	}
	if got := seen.get(); !reflect.DeepEqual(got, []string{"old", "newer"}) {
		t.Fatalf("labels delivered as %v, want commit order [old newer]", got)
	}
	version, _, err := cli.Store.AppState.GetAppStateVersion(t.Context(), string(appstate.WAPatchRegular))
	if err != nil || version != 102 {
		t.Fatalf("version=%d err=%v, want 102", version, err)
	}
}

// A dispatched fetch, a recovery and an ordered captured fetch commit in that
// order while the first one is still inside a handler. Neither later batch
// may wait for it, and all three must be delivered in commit order.
func TestAppStateBatchesPublishInCommitOrder(t *testing.T) {
	cli, server := reviewLabelClient(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	paused, resume := make(chan struct{}), make(chan struct{})
	var gated atomic.Bool
	cli.AddEventHandler(func(evt any) {
		if label, ok := evt.(*events.LabelEdit); ok && label.Action.GetName() == "old" && !gated.Swap(true) {
			close(paused)
			select {
			case <-resume:
			case <-ctx.Done():
			}
		}
	})
	var seen labelLog
	cli.AddEventHandler(func(evt any) { seen.add(evt) })
	done := make(chan error, 1)
	go func() { done <- cli.FetchAppState(ctx, appstate.WAPatchRegular, false, false) }()
	select {
	case <-paused:
	case <-ctx.Done():
		t.Fatal("fetch did not reach its label event")
	}
	reviewAdoptLabelRecovery(t, cli)
	appendLabelPatch(t, cli, server, "newest")
	var captured atomic.Bool
	if err := cli.DangerousInternals().FetchAppStateOrdered(ctx, appstate.WAPatchRegular, false, false, func(evts []any) {
		captured.Store(true)
		seen.add(evts...)
	}); err != nil {
		t.Fatal(err)
	}
	if captured.Load() || len(seen.get()) != 0 {
		t.Fatalf("later batches overtook the batch still being dispatched: captured=%v seen=%v", captured.Load(), seen.get())
	}
	close(resume)
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("fetch wedged")
	}
	if got := seen.get(); !reflect.DeepEqual(got, []string{"old", "new", "newest"}) {
		t.Fatalf("labels delivered as %v, want commit order [old new newest]", got)
	}
	version, hash, err := cli.Store.AppState.GetAppStateVersion(t.Context(), string(appstate.WAPatchRegular))
	if err != nil || version != 111 || hash != sumStoredMACs(t, cli.Store.AppState, appstate.WAPatchRegular) {
		t.Fatalf("version=%d err=%v, want a consistent 111", version, err)
	}
}

func TestAppStatePublisherAbandonAndPanic(t *testing.T) {
	cli := &Client{Log: waLog.Noop}
	name := appstate.WAPatchRegular
	var order []string
	publish := func(label string) func() { return func() { order = append(order, label) } }

	first, second, third := &appStateBatch{name: name}, &appStateBatch{name: name}, &appStateBatch{name: name}
	cli.appStatePublisher.reserve(first)
	cli.appStatePublisher.reserve(second)
	cli.finishAppStateBatch(second, publish("second"))
	cli.finishAppStateBatch(third, publish("third"))
	if len(order) != 0 {
		t.Fatalf("published %v before the earlier commit finished", order)
	}
	// A failed fetch gives up its place; the batches behind it go out in order.
	cli.finishAppStateBatch(first, nil)
	if !reflect.DeepEqual(order, []string{"second", "third"}) {
		t.Fatalf("published %v", order)
	}

	order = nil
	cli.finishAppStateBatch(&appStateBatch{name: name}, func() { panic("handler bug") })
	cli.finishAppStateBatch(&appStateBatch{name: name}, publish("after panic"))
	// Another collection is never held back.
	blocked := &appStateBatch{name: name}
	cli.appStatePublisher.reserve(blocked)
	cli.finishAppStateBatch(&appStateBatch{name: appstate.WAPatchRegularLow}, publish("other collection"))
	if !reflect.DeepEqual(order, []string{"after panic", "other collection"}) {
		t.Fatalf("published %v", order)
	}
	cli.finishAppStateBatch(blocked, nil)
	if len(cli.appStatePublisher.pending) != 0 || len(cli.appStatePublisher.draining) != 0 {
		t.Fatalf("publisher kept state: %+v", &cli.appStatePublisher)
	}
}
