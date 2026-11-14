package whatsmeow

import (
	"bytes"
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/appstate"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/types/events"
)

func TestReviewR3RefusedNotificationResumesAfterDrain(t *testing.T) {
	cli, server := reviewLabelClient(t)
	cli.AppStateBacklogLimit = 1
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	release, fetched, _ := holdLabelFetch(t, ctx, cli)
	var once sync.Once
	resume := func() { once.Do(release) }
	defer resume()
	appendLabelPatch(t, cli, server, "pending")
	before := server.fetches.Load()
	handled := cli.handleAppStateNotification(ctx, &waBinary.Node{
		Tag: "notification",
		Content: []waBinary.Node{{
			Tag: "collection", Attrs: waBinary.Attrs{"name": "regular", "version": "102"},
		}},
	})
	resume()
	if err := <-fetched; err != nil {
		t.Fatal(err)
	}
	if err := cli.WaitForAppStateEvents(ctx); err != nil {
		t.Fatal(err)
	}
	stalled, _, err := cli.Store.AppState.GetAppStateVersion(ctx, "regular")
	if err != nil {
		t.Fatal(err)
	}
	noRetry := server.fetches.Load() == before
	if err := cli.FetchAppState(ctx, appstate.WAPatchRegular, false, false); err != nil {
		t.Fatal(err)
	}
	afterManual, _, err := cli.Store.AppState.GetAppStateVersion(ctx, "regular")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("notification_handled=%t fetches_unchanged_after_drain=%t cursor_after_drain=%d cursor_after_explicit_fetch=%d",
		handled, noRetry, stalled, afterManual)
	if handled && noRetry && stalled != afterManual {
		t.Fatal("refused notification was consumed without resuming catch-up after the backlog drained")
	}
}

func TestReviewR3ConcurrentFetchesRespectBacklogLimit(t *testing.T) {
	cli, _ := reviewLabelClient(t)
	cli.AppStateBacklogLimit = 1
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	entered, release := make(chan struct{}), make(chan struct{})
	var gate, leave sync.Once
	resume := func() { leave.Do(func() { close(release) }) }
	defer resume()
	cli.AddEventHandler(func(evt any) {
		if _, ok := evt.(*events.LabelEdit); ok {
			gate.Do(func() {
				close(entered)
				select {
				case <-release:
				case <-ctx.Done():
				}
			})
		}
	})
	const producers = 16
	done := make(chan error, producers)
	cli.appStateSyncLock.Lock()
	locked := true
	defer func() {
		if locked {
			cli.appStateSyncLock.Unlock()
		}
	}()
	for range producers {
		go func() { done <- cli.FetchAppState(ctx, appstate.WAPatchRegular, false, false) }()
	}
	// Establish that every public fetch passed admission and is waiting for
	// the sync lock before letting the first one commit. No sleeps or
	// production test hooks are needed, and no stacks are printed.
	stack := make([]byte, 1<<20)
	for {
		n := runtime.Stack(stack, true)
		if bytes.Count(stack[:n], []byte("(*Client).fetchAppStateLocked(")) >= producers {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("producers did not reach the controlled sync-lock boundary")
		}
		runtime.Gosched()
	}
	cli.appStateSyncLock.Unlock()
	locked = false
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("first fetch never reached its handler")
	}
	for range producers - 1 {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("admitted producers did not finish")
		}
	}
	depth := cli.AppStateBacklog(appstate.WAPatchRegular).Depth
	resume()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := cli.WaitForAppStateEvents(ctx); err != nil {
		t.Fatal(err)
	}
	t.Logf("configured_limit=1 concurrent_producers=%d observed_depth=%d", producers, depth)
	if depth > 1 {
		t.Fatal("admission checked capacity without reserving it; concurrent entrants exceeded the limit")
	}
}
