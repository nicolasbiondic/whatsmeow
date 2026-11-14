package whatsmeow

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/appstate"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types/events"
)

// owedLabels is a client at v101 of regular whose only backlog slot is taken
// by the returned batch, and that has just refused the notification of v102:
// the collection is owed a fetch.
func owedLabels(t *testing.T, ctx context.Context, notified uint64) (*Client, *fakeSyncdServer, *backlogNotices, *appStateBatch) {
	t.Helper()
	cli, server := reviewLabelClient(t)
	cli.AppStateBacklogLimit = 1
	notices := watchBacklog(cli)
	if err := cli.FetchAppState(ctx, appstate.WAPatchRegular, false, false); err != nil {
		t.Fatal(err)
	}
	appendLabelPatch(t, cli, server, "pending")
	slot := &appStateBatch{name: appstate.WAPatchRegular}
	if err := cli.claimAppStateBatch(slot); err != nil {
		t.Fatal(err)
	}
	if !cli.handleAppStateNotification(ctx, labelNotification(notified)) {
		t.Fatal("a refused notification must be consumed")
	}
	eventually(t, ctx, "the refusal to be reported", func() bool { return len(notices.deferred()) == 1 })
	return cli, server, notices, slot
}

func labelVersion(t *testing.T, cli *Client) uint64 {
	t.Helper()
	version, hash, err := cli.Store.AppState.GetAppStateVersion(t.Context(), string(appstate.WAPatchRegular))
	if err != nil || hash != sumStoredMACs(t, cli.Store.AppState, appstate.WAPatchRegular) {
		t.Fatalf("stored state is not consistent: version=%d err=%v", version, err)
	}
	return version
}

func (p *appStatePublisher) debt(name appstate.WAPatchName) (debt appStateDebt, owed bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.owed[name] == nil {
		return debt, false
	}
	return *p.owed[name], true
}

// An attempt that finds the backlog full again asks the server nothing, sets
// no timer, reports nothing new, and is made again when capacity comes back.
func TestAppStateCatchUpRefusedAgainWaitsForTheNextDrain(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cli, server, notices, slot := owedLabels(t, ctx, 102)
	fetches := server.fetches.Load()
	p := &cli.appStatePublisher

	// Capacity came back and was taken again before the attempt could claim.
	p.mu.Lock()
	p.catchingUp[appstate.WAPatchRegular] = true
	connects := p.connects
	p.mu.Unlock()
	cli.catchUpAppState(appstate.WAPatchRegular, connects)

	debt, owed := p.debt(appstate.WAPatchRegular)
	if !owed || debt.timer != nil || debt.offline || debt.failures != 0 {
		t.Fatalf("debt after a refused attempt: owed=%v %+v", owed, debt)
	}
	p.mu.Lock()
	queued := len(p.notices)
	p.mu.Unlock()
	if server.fetches.Load() != fetches || queued != 0 || len(notices.deferred()) != 1 || len(notices.failed()) != 0 || labelVersion(t, cli) != 101 {
		t.Fatalf("refused attempt was not silent: fetches=%d notices=%d", server.fetches.Load()-fetches, queued)
	}

	cli.finishAppStateBatch(slot, nil)
	if err := cli.WaitForAppStateEvents(ctx); err != nil {
		t.Fatal(err)
	}
	if _, owed := p.debt(appstate.WAPatchRegular); owed || labelVersion(t, cli) != 102 || server.fetches.Load() != fetches+1 || len(notices.caughtUp()) != 1 {
		t.Fatalf("not caught up at the next drain: version=%d fetches=%d", labelVersion(t, cli), server.fetches.Load()-fetches)
	}
}

// failingVersionStore fails the version read of the next fetches.
type failingVersionStore struct {
	store.AppStateStore
	failures atomic.Int32
	attempts atomic.Int32
}

func (s *failingVersionStore) RecoverAppStateBackup(ctx context.Context, name string) error {
	s.attempts.Add(1)
	return s.AppStateStore.RecoverAppStateBackup(ctx, name)
}

func (s *failingVersionStore) GetAppStateVersion(ctx context.Context, name string) (uint64, [128]byte, error) {
	if s.failures.Add(-1) >= 0 {
		return 0, [128]byte{}, errors.New("injected store failure")
	}
	return s.AppStateStore.GetAppStateVersion(ctx, name)
}

func (s *failingVersionStore) GetAllAppStateMutationMACs(ctx context.Context, name string) ([]store.AppStateMutationMAC, error) {
	return s.AppStateStore.(appStateMutationMACDumper).GetAllAppStateMutationMACs(ctx, name)
}

// An attempt that fails for another reason keeps the debt, reports the
// failure, and is made again after a backoff that doubles.
func TestAppStateCatchUpRetriesAfterAFailure(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cli, _, notices, slot := owedLabels(t, ctx, 102)
	failing := &failingVersionStore{AppStateStore: cli.Store.AppState}
	failing.failures.Store(2)
	cli.Store.AppState = failing
	cli.appStatePublisher.retryBase = 20 * time.Millisecond

	cli.finishAppStateBatch(slot, nil)
	// The wait covers the attempt, not the retry timer it leaves behind.
	if err := cli.WaitForAppStateEvents(ctx); err != nil {
		t.Fatal(err)
	}
	if _, owed := cli.appStatePublisher.debt(appstate.WAPatchRegular); !owed || len(notices.failed()) == 0 {
		t.Fatalf("debt after a failed attempt: owed=%v failed=%d", owed, len(notices.failed()))
	}
	eventually(t, ctx, "the retries to catch up", func() bool { return len(notices.caughtUp()) == 1 })
	failed := notices.failed()
	if len(failed) != 2 || failed[0].RetryIn != 20*time.Millisecond || failed[1].RetryIn != 40*time.Millisecond || failed[0].Error == nil || failed[0].Version != 102 {
		t.Fatalf("failures reported as %+v", failed)
	}
	if _, owed := cli.appStatePublisher.debt(appstate.WAPatchRegular); owed || labelVersion(t, cli) != 102 || failing.attempts.Load() != 3 {
		t.Fatalf("version=%d attempts=%d", labelVersion(t, cli), failing.attempts.Load())
	}
}

// Disconnect parks what is owed: no attempt is made, and no store is read,
// until the next connect, also when that connect comes while an earlier
// attempt is still failing.
func TestAppStateCatchUpWaitsForTheConnection(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cli, server, notices, slot := owedLabels(t, ctx, 102)
	counting := &failingVersionStore{AppStateStore: cli.Store.AppState}
	cli.Store.AppState = counting
	p := &cli.appStatePublisher
	cli.Disconnect()

	debt, owed := p.debt(appstate.WAPatchRegular)
	if !owed || !debt.offline || debt.timer != nil {
		t.Fatalf("debt after Disconnect: owed=%v %+v", owed, debt)
	}
	// Capacity coming back, and more drains, do not start an attempt.
	cli.finishAppStateBatch(slot, nil)
	cli.finishAppStateBatch(&appStateBatch{name: appstate.WAPatchRegular}, func() {})
	if err := cli.WaitForAppStateEvents(ctx); err != nil || counting.attempts.Load() != 0 {
		t.Fatalf("attempt without a connection: attempts=%d err=%v", counting.attempts.Load(), err)
	}
	// The application is told once that nothing is scheduled.
	failed := notices.failed()
	if len(failed) != 1 || failed[0].RetryIn != 0 || !errors.Is(failed[0].Error, ErrNotConnected) || failed[0].Version != 102 {
		t.Fatalf("parking reported as %+v", failed)
	}
	// An attempt that slips through checks the connection before any store.
	p.mu.Lock()
	p.catchingUp[appstate.WAPatchRegular] = true
	connects := p.connects
	p.mu.Unlock()
	cli.catchUpAppState(appstate.WAPatchRegular, connects)
	if counting.attempts.Load() != 0 || len(notices.failed()) != 1 {
		t.Fatalf("disconnected attempt read the store or reported again: attempts=%d", counting.attempts.Load())
	}

	// A connect that comes while an attempt is failing must not be lost.
	p.mu.Lock()
	p.catchingUp[appstate.WAPatchRegular] = true
	stale := p.connects
	p.mu.Unlock()
	cli.resumeAppStateCatchUps()
	cli.endAppStateCatchUp(appstate.WAPatchRegular, stale, ErrNotConnected)
	if err := cli.WaitForAppStateEvents(ctx); err != nil {
		t.Fatal(err)
	}
	if debt, _ := p.debt(appstate.WAPatchRegular); !debt.offline || counting.attempts.Load() != 0 {
		t.Fatalf("connect during a failing attempt: %+v attempts=%d, want one attempt that found no connection", debt, counting.attempts.Load())
	}

	// Reconnect to a peer that has the same patch log, through the client's
	// own handling of the server's login confirmation. That peer answers no
	// other request, so the unrelated ones it makes first get little time.
	reconnected := attachFakeServer(t, cli, appstate.WAPatchRegular)
	server.mu.Lock()
	reconnected.version, reconnected.patches = server.version, server.patches
	server.mu.Unlock()
	login, loggedIn := context.WithTimeout(ctx, 200*time.Millisecond)
	defer loggedIn()
	cli.handleConnectSuccess(login, &waBinary.Node{Tag: "success", Attrs: waBinary.Attrs{"t": time.Now().Unix()}})
	eventually(t, ctx, "the catch-up after the reconnect", func() bool { return len(notices.caughtUp()) == 1 })
	if err := cli.WaitForAppStateEvents(ctx); err != nil {
		t.Fatal(err)
	}
	if _, owed := p.debt(appstate.WAPatchRegular); owed || labelVersion(t, cli) != 102 || reconnected.fetches.Load() != 1 || len(notices.caughtUp()) != 1 {
		t.Fatalf("not caught up after the reconnect: version=%d fetches=%d", labelVersion(t, cli), reconnected.fetches.Load())
	}
}

// A fetch that keeps failing is not retried forever: after a few attempts it
// says so once and waits for the next connection, or for any other fetch.
func TestAppStateCatchUpGivesUpUntilTheNextConnection(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cli, _, notices, slot := owedLabels(t, ctx, 102)
	log := &errorLog{}
	cli.Log = log
	failing := &failingVersionStore{AppStateStore: cli.Store.AppState}
	failing.failures.Store(1000)
	cli.Store.AppState = failing
	p := &cli.appStatePublisher
	p.retryBase = time.Millisecond

	cli.finishAppStateBatch(slot, nil)
	eventually(t, ctx, "the catch-up to give up", func() bool {
		debt, _ := p.debt(appstate.WAPatchRegular)
		return debt.offline
	})
	if err := cli.WaitForAppStateEvents(ctx); err != nil {
		t.Fatal(err)
	}
	// More drains change nothing.
	cli.finishAppStateBatch(&appStateBatch{name: appstate.WAPatchRegular}, func() {})
	if err := cli.WaitForAppStateEvents(ctx); err != nil {
		t.Fatal(err)
	}
	debt, owed := p.debt(appstate.WAPatchRegular)
	failed := notices.failed()
	last := failed[len(failed)-1]
	if !owed || debt.timer != nil || failing.attempts.Load() != appStateCatchUpMaxFailures || last.RetryIn != 0 || !strings.Contains(last.Error.Error(), "gave up after 5 attempts") {
		t.Fatalf("owed=%v debt=%+v attempts=%d last notice=%+v", owed, debt, failing.attempts.Load(), last)
	}
	lines := log.get()
	if len(lines) != appStateCatchUpMaxFailures || !strings.Contains(lines[len(lines)-1], "Giving up on catching up app state regular") {
		t.Fatalf("ERROR lines: %q", lines)
	}

	failing.failures.Store(0)
	cli.resumeAppStateCatchUps()
	if err := cli.WaitForAppStateEvents(ctx); err != nil {
		t.Fatal(err)
	}
	if _, owed := p.debt(appstate.WAPatchRegular); owed || labelVersion(t, cli) != 102 || len(notices.caughtUp()) != 1 {
		t.Fatalf("not caught up after the next connect: version=%d", labelVersion(t, cli))
	}
}

// Any fetch of the collection that asked the server after the refusal
// settles the debt, and the client does not fetch again for it. A fetch that
// had asked before the refusal does not.
func TestOrdinaryFetchSettlesAppStateDebt(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cli, server, notices, slot := owedLabels(t, ctx, 102)
	p := &cli.appStatePublisher
	failing := &failingVersionStore{AppStateStore: cli.Store.AppState}
	failing.failures.Store(1)
	cli.Store.AppState = failing
	p.retryBase = time.Hour

	// A round with the server that began before the refusal knows nothing of it.
	cli.settleAppStateCatchUp(appstate.WAPatchRegular, 0)
	if _, owed := p.debt(appstate.WAPatchRegular); !owed {
		t.Fatal("a fetch that asked before the refusal settled the debt")
	}

	// Park the debt on a long backoff, then fetch as any caller would.
	cli.finishAppStateBatch(slot, nil)
	if err := cli.WaitForAppStateEvents(ctx); err != nil {
		t.Fatal(err)
	}
	if debt, _ := p.debt(appstate.WAPatchRegular); debt.timer == nil {
		t.Fatalf("debt is not waiting for its retry: %+v", debt)
	}
	fetches := server.fetches.Load()
	// A fetch that is skipped because the collection is synced asks nothing.
	if err := cli.FetchAppState(ctx, appstate.WAPatchRegular, false, true); err != nil {
		t.Fatal(err)
	}
	if _, owed := p.debt(appstate.WAPatchRegular); !owed || server.fetches.Load() != fetches {
		t.Fatal("a fetch that did not ask the server settled the debt")
	}
	if err := cli.FetchAppState(ctx, appstate.WAPatchRegular, false, false); err != nil {
		t.Fatal(err)
	}
	if err := cli.WaitForAppStateEvents(ctx); err != nil {
		t.Fatal(err)
	}
	if _, owed := p.debt(appstate.WAPatchRegular); owed || labelVersion(t, cli) != 102 || len(notices.caughtUp()) != 1 {
		t.Fatalf("ordinary fetch did not settle the debt: version=%d", labelVersion(t, cli))
	}
	// Everything that could resume a debt finds none.
	cli.resumeAppStateCatchUps()
	cli.finishAppStateBatch(&appStateBatch{name: appstate.WAPatchRegular}, func() {})
	if err := cli.WaitForAppStateEvents(ctx); err != nil || server.fetches.Load() != fetches+1 || failing.attempts.Load() != 3 {
		t.Fatalf("duplicate fetch after the debt was settled: fetches=%d attempts=%d err=%v", server.fetches.Load()-fetches, failing.attempts.Load(), err)
	}
}

// The notified version is only a diagnostic. After a server reset to a lower
// cursor it is never reached, and the debt must still end with one fetch.
func TestAppStateDebtDoesNotCompareVersions(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cli, server, notices, slot := owedLabels(t, ctx, 5000)
	fetches := server.fetches.Load()
	cli.finishAppStateBatch(slot, nil)
	if err := cli.WaitForAppStateEvents(ctx); err != nil {
		t.Fatal(err)
	}
	cli.finishAppStateBatch(&appStateBatch{name: appstate.WAPatchRegular}, func() {})
	if err := cli.WaitForAppStateEvents(ctx); err != nil {
		t.Fatal(err)
	}
	caughtUp := notices.caughtUp()
	if _, owed := cli.appStatePublisher.debt(appstate.WAPatchRegular); owed || labelVersion(t, cli) != 102 || server.fetches.Load() != fetches+1 || len(caughtUp) != 1 || caughtUp[0].Version != 5000 {
		t.Fatalf("debt below the notified version: version=%d fetches=%d caughtUp=%+v", labelVersion(t, cli), server.fetches.Load()-fetches, caughtUp)
	}
}

// A 409 conflict batch refused by the backlog fails the send, stores
// nothing, and leaves the conflicting patches owed.
func TestRefusedConflictBatchIsOwed(t *testing.T) {
	env := newAppState409RaceEnv(t, newSQLiteAppStateDevice(t))
	cli := env.cli
	cli.AppStateBacklogLimit = 1
	notices := watchBacklog(cli)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	// The slot is taken while the rejected send is on its way back.
	slot := &appStateBatch{name: env.name}
	env.server.beforeConflict = func() { cli.appStatePublisher.reserve(slot) }
	err := cli.SendAppState(ctx, appstate.BuildArchive(env.other, true, time.Now(), nil))
	if !errors.Is(err, ErrAppStateBacklog) || !errors.Is(err, ErrAppStateUpdate) {
		t.Fatalf("send whose conflict batch was refused: %v", err)
	}
	env.checkConsistent(t, 100)
	eventually(t, ctx, "the refusal to be reported", func() bool { return len(notices.deferred()) == 1 })
	if deferred := notices.deferred()[0]; deferred.Source != "conflict" || deferred.Name != env.name {
		t.Fatalf("refusal reported as %+v", deferred)
	}
	cli.finishAppStateBatch(slot, nil)
	if err := cli.WaitForAppStateEvents(ctx); err != nil {
		t.Fatal(err)
	}
	env.checkConsistent(t, 101)
	if len(notices.caughtUp()) != 1 || env.server.sends.Load() != 1 {
		t.Fatalf("caughtUp=%d sends=%d", len(notices.caughtUp()), env.server.sends.Load())
	}
}

// panickingStore panics in the version read of a fetch.
type panickingStore struct {
	store.AppStateStore
}

func (panickingStore) GetAppStateVersion(context.Context, string) (uint64, [128]byte, error) {
	panic("injected store panic")
}

// Capacity that was claimed comes back on every way out that delivers
// nothing: an error, a cancellation, nothing to do, a panic.
func TestAppStateClaimIsReleasedOnEveryPath(t *testing.T) {
	cancelled, cancelNow := context.WithCancel(t.Context())
	cancelNow()
	first := appStateOrigin{source: "test", first: true}
	paths := []struct {
		name string
		run  func(t *testing.T, cli *Client, server *fakeSyncdServer)
	}{
		{"claimed fetch that finds nothing", func(t *testing.T, cli *Client, _ *fakeSyncdServer) {
			if err := cli.fetchAppStateFrom(t.Context(), appstate.WAPatchRegular, false, false, first); err != nil {
				t.Fatal(err)
			}
		}},
		{"claimed fetch cancelled", func(t *testing.T, cli *Client, _ *fakeSyncdServer) {
			if err := cli.fetchAppStateFrom(cancelled, appstate.WAPatchRegular, false, false, first); err == nil {
				t.Fatal("cancelled fetch succeeded")
			}
		}},
		{"full sync that fails after its reset", func(t *testing.T, cli *Client, _ *fakeSyncdServer) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			original := cli.Store.AppState
			cli.Store.AppState = &cancelAfterResetStore{AppStateStore: original, cancel: cancel}
			err := cli.FetchAppState(ctx, appstate.WAPatchRegular, true, false)
			cli.Store.AppState = original
			if !errors.Is(err, context.Canceled) || labelVersion(t, cli) != 101 {
				t.Fatalf("full sync cancelled after its reset: %v", err)
			}
		}},
		{"fetch that fails to apply", func(t *testing.T, cli *Client, server *fakeSyncdServer) {
			// A patch that does not verify against the stored state.
			keyID, err := cli.Store.AppStateKeys.GetLatestAppStateSyncKeyID(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			raw, err := cli.appStateProc.EncodePatch(t.Context(), keyID, appstate.HashState{Version: 101}, appstate.BuildLabelEdit("8", "forged", 1, false))
			if err != nil {
				t.Fatal(err)
			}
			server.appendPatch(raw)
			if err := cli.FetchAppState(t.Context(), appstate.WAPatchRegular, false, false); !errors.Is(err, appstate.ErrMismatchingLTHash) {
				t.Fatalf("patch that does not verify: %v", err)
			}
		}},
		{"fetch that panics", func(t *testing.T, cli *Client, _ *fakeSyncdServer) {
			cli.Store.AppState = panickingStore{cli.Store.AppState}
			defer func() {
				if recover() == nil {
					t.Fatal("fetch did not panic")
				}
			}()
			_ = cli.fetchAppStateFrom(t.Context(), appstate.WAPatchRegular, false, false, first)
		}},
		{"send that panics", func(t *testing.T, cli *Client, _ *fakeSyncdServer) {
			cli.Store.AppState = panickingStore{cli.Store.AppState}
			defer func() {
				if recover() == nil {
					t.Fatal("send did not panic")
				}
			}()
			_ = cli.SendAppState(t.Context(), appstate.BuildLabelEdit("7", "local", 1, false))
		}},
		{"send cancelled", func(t *testing.T, cli *Client, _ *fakeSyncdServer) {
			if err := cli.SendAppState(cancelled, appstate.BuildLabelEdit("7", "local", 1, false)); err == nil {
				t.Fatal("cancelled send succeeded")
			}
		}},
		{"recovery older than the stored state", func(t *testing.T, cli *Client, _ *fakeSyncdServer) {
			if !cli.handleAppStateRecovery(t.Context(), "old-recovery", labelRecovery(t, cli, 50, "older")) {
				t.Fatal("ignored recovery reported as not handled")
			}
		}},
		{"captured fetch that commits", func(t *testing.T, cli *Client, server *fakeSyncdServer) {
			appendLabelPatch(t, cli, server, "captured")
			evts, err := cli.DangerousInternals().FetchAppState(t.Context(), appstate.WAPatchRegular, false, false)
			if err != nil || len(evts) == 0 {
				t.Fatalf("captured fetch: %d events, %v", len(evts), err)
			}
		}},
	}
	for _, path := range paths {
		t.Run(path.name, func(t *testing.T) {
			cli, server := reviewLabelClient(t)
			cli.AppStateBacklogLimit = 1
			if err := cli.FetchAppState(t.Context(), appstate.WAPatchRegular, false, false); err != nil {
				t.Fatal(err)
			}
			path.run(t, cli, server)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			if err := cli.WaitForAppStateEvents(ctx); err != nil {
				t.Fatalf("something is still held: %v", err)
			}
			p := &cli.appStatePublisher
			p.mu.Lock()
			defer p.mu.Unlock()
			if backlog := p.backlogLocked(appstate.WAPatchRegular); backlog.Depth != 0 || len(p.claims) != 0 || len(p.pending) != 0 {
				t.Fatalf("capacity was not given back: %+v claims=%v", backlog, p.claims)
			}
		})
	}
}

// Sends, recovery adoptions and fetches wanting the capacity behind a held
// handler cannot exceed the limit together, and give all of it back.
func TestMixedAppStateProducersCannotExceedTheLimit(t *testing.T) {
	const limit, producers = 3, 8
	cli, server := reviewLabelClient(t)
	cli.AppStateBacklogLimit = limit
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Second)
	defer cancel()
	release, fetched, _ := holdLabelFetch(t, ctx, cli)
	appendLabelPatch(t, cli, server, "pending")

	// Every send conflicts with "pending". The server holds its answers, so
	// the sends that were admitted keep their capacity while the others ask.
	answer := make(chan struct{})
	server.beforeConflict = func() { <-answer }
	sent := make(chan error, producers)
	for range producers {
		go func() { sent <- cli.SendAppState(ctx, appstate.BuildLabelEdit("7", "local", 1, false)) }()
	}
	for range producers - (limit - 1) {
		if err := <-sent; !errors.Is(err, ErrAppStateBacklog) {
			t.Fatalf("send over the limit: %v", err)
		}
	}
	eventually(t, ctx, "the admitted sends to reach the server", func() bool { return server.conflicts.Load() == limit-1 })
	var others sync.WaitGroup
	var refused atomic.Int32
	for i := range producers {
		recovery := labelRecovery(t, cli, uint64(200+i), "recovered")
		others.Go(func() { cli.handleAppStateRecovery(ctx, "racing-recovery", recovery) })
		others.Go(func() {
			if errors.Is(cli.FetchAppState(ctx, appstate.WAPatchRegular, false, false), ErrAppStateBacklog) {
				refused.Add(1)
			}
		})
	}
	others.Wait()
	// No recovery was adopted either: the newest one is kept for later.
	p := &cli.appStatePublisher
	p.mu.Lock()
	kept := p.recoveries[appstate.WAPatchRegular]
	p.mu.Unlock()
	if depth := cli.AppStateBacklog(appstate.WAPatchRegular).Depth; depth != limit || refused.Load() != producers || labelVersion(t, cli) != 101 || kept == nil || kept.version != 200+producers-1 {
		t.Fatalf("limit=%d depth=%d refused fetches=%d of %d kept recovery=%+v", limit, depth, refused.Load(), producers, kept)
	}

	// The admitted sends go on: conflict batch, retry, its events.
	close(answer)
	for range limit - 1 {
		<-sent
	}
	if depth := cli.AppStateBacklog(appstate.WAPatchRegular).Depth; depth > limit {
		t.Fatalf("limit=%d depth=%d after the sends", limit, depth)
	}
	release()
	if err := <-fetched; err != nil {
		t.Fatal(err)
	}
	if err := cli.WaitForAppStateEvents(ctx); err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if backlog := p.backlogLocked(appstate.WAPatchRegular); backlog.Depth != 0 || len(p.claims) != 0 || len(p.owed) != 0 || len(p.recoveries) != 0 {
		t.Fatalf("capacity was not given back: %+v claims=%v owed=%d recoveries=%d", backlog, p.claims, len(p.owed), len(p.recoveries))
	}
}

// While a handler keeps the notices from being delivered they do not pile
// up: the latest of each kind per collection is kept, so what arrives last
// for a collection is its latest state. Every refusal is still logged.
func TestAppStateNoticesAreCoalesced(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cli, _, notices, slot := owedLabels(t, ctx, 102)
	log := &errorLog{}
	cli.Log = log
	held, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	cli.AddEventHandler(func(evt any) {
		if _, ok := evt.(*events.AppStateDeferred); ok {
			once.Do(func() {
				close(held)
				<-release
			})
		}
	})
	const refusals = 200
	for i := range refusals {
		cli.handleAppStateNotification(ctx, labelNotification(uint64(103+i)))
		if i == 0 {
			<-held
		}
	}
	p := &cli.appStatePublisher
	p.mu.Lock()
	queued := append([]any(nil), p.notices...)
	p.mu.Unlock()
	if len(queued) != 1 || queued[0].(*events.AppStateDeferred).Version != 103+refusals-1 || len(log.get()) != refusals {
		t.Fatalf("queued notices=%d, ERROR lines=%d of %d", len(queued), len(log.get()), refusals)
	}
	// A different kind is kept beside it, and a newer one moves to the end.
	p.mu.Lock()
	cli.noticeAppStateLocked(&events.AppStateCaughtUp{Name: appstate.WAPatchRegular, Version: 1})
	cli.noticeAppStateLocked(&events.AppStateDeferred{Name: appstate.WAPatchRegularLow})
	cli.noticeAppStateLocked(&events.AppStateDeferred{Name: appstate.WAPatchRegular, Version: 9000})
	order := make([]string, len(p.notices))
	for i, queued := range p.notices {
		kind, name := appStateNoticeKey(queued)
		order[i] = fmt.Sprint(kind, name)
	}
	p.mu.Unlock()
	if want := []string{"2regular", "1regular_low", "1regular"}; !reflect.DeepEqual(order, want) {
		t.Fatalf("queued notices %v, want %v", order, want)
	}
	close(release)
	cli.finishAppStateBatch(slot, nil)
	if err := cli.WaitForAppStateEvents(ctx); err != nil {
		t.Fatal(err)
	}
	deferred := notices.deferred()
	if last := deferred[len(deferred)-1]; last.Name != appstate.WAPatchRegular || last.Version != 9000 || labelVersion(t, cli) != 102 {
		t.Fatalf("last refusal delivered: %+v, version %d", last, labelVersion(t, cli))
	}
}

// A socket that the server or the network closed parks what is owed, like
// Disconnect does: the retry that was armed does not fire into a client
// that has no connection.
func TestLostSocketParksAppStateDebts(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cli, _, notices, slot := owedLabels(t, ctx, 102)
	cli.EnableAutoReconnect = false
	failing := &failingVersionStore{AppStateStore: cli.Store.AppState}
	failing.failures.Store(1)
	cli.Store.AppState = failing
	p := &cli.appStatePublisher
	p.retryBase = 50 * time.Millisecond
	cli.finishAppStateBatch(slot, nil)
	eventually(t, ctx, "the failed attempt", func() bool { return len(notices.failed()) == 1 })

	cli.socketLock.RLock()
	lost := cli.socket
	cli.socketLock.RUnlock()
	cli.onDisconnect(ctx, lost, true)
	debt, owed := p.debt(appstate.WAPatchRegular)
	if !owed || !debt.offline || debt.timer != nil {
		t.Fatalf("debt after the socket was lost: owed=%v %+v", owed, debt)
	}
	time.Sleep(150 * time.Millisecond)
	if err := cli.WaitForAppStateEvents(ctx); err != nil {
		t.Fatal(err)
	}
	failed := notices.failed()
	if failing.attempts.Load() != 1 || len(failed) != 2 || failed[1].RetryIn != 0 || !errors.Is(failed[1].Error, ErrNotConnected) {
		t.Fatalf("attempts=%d notices=%+v", failing.attempts.Load(), failed)
	}
	// A socket that is not the current one changes nothing.
	cli.resumeAppStateCatchUps()
	cli.onDisconnect(ctx, lost, true)
	if err := cli.WaitForAppStateEvents(ctx); err != nil {
		t.Fatal(err)
	}
	p.mu.Lock()
	disconnected := p.disconnected
	p.mu.Unlock()
	if disconnected {
		t.Fatal("an old socket's end parked the debts of the current connection")
	}
}

// Work refused while the client is disconnected is owed like any other, and
// waits for the connection from the start: capacity coming back starts
// nothing, and no store is read.
func TestAppStateDebtOfADisconnectedClientWaitsForTheConnection(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cli, server := reviewLabelClient(t)
	cli.AppStateBacklogLimit = 1
	notices := watchBacklog(cli)
	if err := cli.FetchAppState(ctx, appstate.WAPatchRegular, false, false); err != nil {
		t.Fatal(err)
	}
	appendLabelPatch(t, cli, server, "pending")
	counting := &failingVersionStore{AppStateStore: cli.Store.AppState}
	cli.Store.AppState = counting
	slot := &appStateBatch{name: appstate.WAPatchRegular}
	if err := cli.claimAppStateBatch(slot); err != nil {
		t.Fatal(err)
	}
	cli.Disconnect()
	if err := cli.DangerousInternals().FetchAppStateOrdered(ctx, appstate.WAPatchRegular, false, false, func([]any) {}); !errors.Is(err, ErrAppStateBacklog) {
		t.Fatalf("ordered fetch at the limit: %v", err)
	}
	p := &cli.appStatePublisher
	if debt, owed := p.debt(appstate.WAPatchRegular); !owed || !debt.offline || debt.timer != nil {
		t.Fatalf("debt of a disconnected client: owed=%v %+v", owed, debt)
	}
	cli.finishAppStateBatch(slot, nil)
	if err := cli.WaitForAppStateEvents(ctx); err != nil || counting.attempts.Load() != 0 || len(notices.failed()) != 0 {
		t.Fatalf("disconnected client tried to catch up: attempts=%d notices=%d err=%v", counting.attempts.Load(), len(notices.failed()), err)
	}
	reconnected := attachFakeServer(t, cli, appstate.WAPatchRegular)
	server.mu.Lock()
	reconnected.version, reconnected.patches = server.version, server.patches
	server.mu.Unlock()
	cli.resumeAppStateCatchUps()
	if err := cli.WaitForAppStateEvents(ctx); err != nil {
		t.Fatal(err)
	}
	if _, owed := p.debt(appstate.WAPatchRegular); owed || labelVersion(t, cli) != 102 || len(notices.caughtUp()) != 1 {
		t.Fatalf("not caught up after the connection: version=%d", labelVersion(t, cli))
	}
}
