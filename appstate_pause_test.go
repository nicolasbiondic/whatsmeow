package whatsmeow

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/appstate"
)

type pauseResult struct {
	unfinished map[appstate.WAPatchName]AppStateUnfinished
	err        error
}

func pauseInBackground(t *testing.T, ctx context.Context, cli *Client) <-chan pauseResult {
	t.Helper()
	done := make(chan pauseResult, 1)
	go func() {
		unfinished, err := cli.PauseAppState(ctx)
		done <- pauseResult{unfinished, err}
	}()
	eventually(t, ctx, "app state to be paused", cli.appStatePaused)
	return done
}

// PauseAppState is the shutdown barrier: work that arrives while it waits is
// refused before it sends or stores anything, without the alarm of a backlog
// refusal and without being lost; everything that was committed is delivered
// before it returns; nothing is caught up until the resume.
func TestPauseAppStateFencesThenDrains(t *testing.T) {
	cli, server := reviewLabelClient(t)
	log := &errorLog{}
	cli.Log = log
	notices := watchBacklog(cli)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	release, fetched, seen := holdLabelFetch(t, ctx, cli)
	reviewAdoptLabelRecovery(t, cli)
	appendLabelPatch(t, cli, server, "pending")
	paused := pauseInBackground(t, ctx, cli)

	fetches := server.fetches.Load()
	if !cli.handleAppStateNotification(ctx, labelNotification(111)) {
		t.Fatal("a notification refused by the pause must be consumed: its fetch is owed")
	}
	if err := cli.FetchAppState(ctx, appstate.WAPatchRegular, false, false); !errors.Is(err, ErrAppStatePaused) {
		t.Fatalf("fetch during the pause: %v", err)
	}
	if err := cli.SendAppState(ctx, appstate.BuildLabelEdit("7", "local", 1, false)); !errors.Is(err, ErrAppStatePaused) {
		t.Fatalf("send during the pause: %v", err)
	}
	if !cli.handleAppStateRecovery(ctx, "during-pause", labelRecovery(t, cli, 130, "dropped")) {
		t.Fatal("a recovery refused by the pause was reported as a handler failure")
	}
	if server.fetches.Load() != fetches || server.sends.Load() != 0 || labelVersion(t, cli) != 110 {
		t.Fatalf("refused work reached the server or the store: fetches=%d sends=%d version=%d", server.fetches.Load()-fetches, server.sends.Load(), labelVersion(t, cli))
	}
	select {
	case got := <-paused:
		t.Fatalf("PauseAppState returned while a handler was held: %+v", got)
	default:
	}

	release()
	if err := <-fetched; err != nil {
		t.Fatal(err)
	}
	got := <-paused
	// Read right after it returned: the committed batches were delivered.
	if delivered := seen.get(); got.err != nil || !reflect.DeepEqual(delivered, []string{"old", "new"}) {
		t.Fatalf("PauseAppState returned err=%v with %v delivered", got.err, delivered)
	}
	want := map[appstate.WAPatchName]AppStateUnfinished{appstate.WAPatchRegular: {Owed: true, RecoveryDropped: true}}
	if !reflect.DeepEqual(got.unfinished, want) {
		t.Fatalf("unfinished=%+v, want %+v", got.unfinished, want)
	}
	if len(log.get()) != 0 || len(notices.deferred()) != 0 {
		t.Fatalf("a pause refusal raised the backlog alarm: log=%q notices=%d", log.get(), len(notices.deferred()))
	}
	// Still paused: capacity is back, and nothing is caught up.
	cli.finishAppStateBatch(&appStateBatch{name: appstate.WAPatchRegular}, func() {})
	if again, err := cli.PauseAppState(ctx); err != nil || !reflect.DeepEqual(again, want) || server.fetches.Load() != fetches {
		t.Fatalf("paused client caught up: unfinished=%+v err=%v fetches=%d", again, err, server.fetches.Load()-fetches)
	}

	cli.ResumeAppState()
	if err := cli.WaitForAppStateEvents(ctx); err != nil {
		t.Fatal(err)
	}
	if _, owed := cli.appStatePublisher.debt(appstate.WAPatchRegular); owed || labelVersion(t, cli) != 111 || !reflect.DeepEqual(seen.get(), []string{"old", "new", "pending"}) || len(notices.caughtUp()) != 1 {
		t.Fatalf("not caught up after the resume: version=%d seen=%v", labelVersion(t, cli), seen.get())
	}
	if err := cli.FetchAppState(ctx, appstate.WAPatchRegular, false, false); err != nil {
		t.Fatalf("fetch after the resume: %v", err)
	}
}

// With a handler that does not return, the pause ends with its context and
// says what is left. It stays in force, and can be asked again.
func TestPauseAppStateReportsWhatAHeldHandlerLeft(t *testing.T) {
	cli, _ := reviewLabelClient(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	release, fetched, seen := holdLabelFetch(t, ctx, cli)
	reviewAdoptLabelRecovery(t, cli)

	short, cancelShort := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancelShort()
	unfinished, err := cli.PauseAppState(short)
	want := map[appstate.WAPatchName]AppStateUnfinished{appstate.WAPatchRegular: {Undelivered: 2}}
	if !errors.Is(err, context.DeadlineExceeded) || !reflect.DeepEqual(unfinished, want) {
		t.Fatalf("unfinished=%+v err=%v, want %+v after the deadline", unfinished, err, want)
	}
	if err := cli.FetchAppState(ctx, appstate.WAPatchRegularLow, false, false); !errors.Is(err, ErrAppStatePaused) {
		t.Fatalf("fetch after a pause that timed out: %v", err)
	}
	release()
	if err := <-fetched; err != nil {
		t.Fatal(err)
	}
	unfinished, err = cli.PauseAppState(ctx)
	if err != nil || len(unfinished) != 1 || unfinished[appstate.WAPatchRegularLow] != (AppStateUnfinished{Owed: true}) || !reflect.DeepEqual(seen.get(), []string{"old", "new"}) {
		t.Fatalf("unfinished=%+v err=%v seen=%v", unfinished, err, seen.get())
	}
}

// A retry that was scheduled does not fire during the pause.
func TestPauseAppStateCancelsRetries(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	cli, _, notices, slot := owedLabels(t, ctx, 102)
	failing := &failingVersionStore{AppStateStore: cli.Store.AppState}
	failing.failures.Store(1)
	cli.Store.AppState = failing
	p := &cli.appStatePublisher
	p.retryBase = 200 * time.Millisecond
	cli.finishAppStateBatch(slot, nil)
	eventually(t, ctx, "the failed attempt", func() bool { return len(notices.failed()) == 1 })

	unfinished, err := cli.PauseAppState(ctx)
	debt, owed := p.debt(appstate.WAPatchRegular)
	if err != nil || unfinished[appstate.WAPatchRegular] != (AppStateUnfinished{Owed: true}) || !owed || debt.timer != nil {
		t.Fatalf("unfinished=%+v err=%v debt=%+v", unfinished, err, debt)
	}
	time.Sleep(300 * time.Millisecond)
	if failing.attempts.Load() != 1 {
		t.Fatalf("a retry ran during the pause: attempts=%d", failing.attempts.Load())
	}
	cli.ResumeAppState()
	if err := cli.WaitForAppStateEvents(ctx); err != nil {
		t.Fatal(err)
	}
	if _, owed := p.debt(appstate.WAPatchRegular); owed || failing.attempts.Load() != 2 || labelVersion(t, cli) != 102 {
		t.Fatalf("not caught up after the resume: attempts=%d version=%d", failing.attempts.Load(), labelVersion(t, cli))
	}
}

// A recovery snapshot refused by the backlog is kept and adopted when the
// backlog has capacity again. No fetch stands in for it, and of several
// refused ones only the newest is adopted.
func TestDeferredRecoveryIsAdoptedAfterTheDrain(t *testing.T) {
	cli, server := reviewLabelClient(t)
	cli.AppStateBacklogLimit = 1
	log := &errorLog{}
	cli.Log = log
	notices := watchBacklog(cli)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	release, fetched, seen := holdLabelFetch(t, ctx, cli)
	for version, label := range map[uint64]string{110: "older", 115: "newest"} {
		if !cli.handleAppStateRecovery(ctx, "deferred", labelRecovery(t, cli, version, label)) {
			t.Fatal("deferred recovery reported as a handler failure")
		}
	}
	if !cli.handleAppStateRecovery(ctx, "deferred", labelRecovery(t, cli, 112, "late but older")) {
		t.Fatal("deferred recovery reported as a handler failure")
	}
	p := &cli.appStatePublisher
	p.mu.Lock()
	kept, owed := p.recoveries[appstate.WAPatchRegular], len(p.owed)
	p.mu.Unlock()
	if kept == nil || kept.version != 115 || owed != 0 || labelVersion(t, cli) != 101 || len(log.get()) != 3 {
		t.Fatalf("kept=%+v fetches owed=%d version=%d log=%q", kept, owed, labelVersion(t, cli), log.get())
	}
	fetches := server.fetches.Load()
	release()
	if err := <-fetched; err != nil {
		t.Fatal(err)
	}
	if err := cli.WaitForAppStateEvents(ctx); err != nil {
		t.Fatal(err)
	}
	caughtUp := notices.caughtUp()
	if labelVersion(t, cli) != 115 || !reflect.DeepEqual(seen.get(), []string{"old", "newest"}) || len(caughtUp) != 1 || caughtUp[0].Version != 115 || server.fetches.Load() != fetches || len(notices.failed()) != 0 {
		t.Fatalf("version=%d seen=%v caughtUp=%+v fetches=%d", labelVersion(t, cli), seen.get(), caughtUp, server.fetches.Load()-fetches)
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.recoveries) != 0 || len(p.dropped) != 0 || len(p.claims) != 0 {
		t.Fatalf("recoveries=%d dropped=%v claims=%v", len(p.recoveries), p.dropped, p.claims)
	}
}

// A deferred snapshot that cannot be adopted after all is reported and
// dropped, as it would have been on arrival. It is not tried again.
func TestDeferredRecoveryThatFailsIsDroppedOnce(t *testing.T) {
	cli, _ := reviewLabelClient(t)
	cli.AppStateBacklogLimit = 1
	notices := watchBacklog(cli)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	release, fetched, seen := holdLabelFetch(t, ctx, cli)
	// Older than what the held fetch stored: found out at adoption.
	if !cli.handleAppStateRecovery(ctx, "superseded", labelRecovery(t, cli, 100, "superseded")) {
		t.Fatal("deferred recovery reported as a handler failure")
	}
	log := &errorLog{}
	cli.Log = log
	release()
	if err := <-fetched; err != nil {
		t.Fatal(err)
	}
	if err := cli.WaitForAppStateEvents(ctx); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		cli.finishAppStateBatch(&appStateBatch{name: appstate.WAPatchRegular}, func() {})
		if err := cli.WaitForAppStateEvents(ctx); err != nil {
			t.Fatal(err)
		}
	}
	failed, lines := notices.failed(), log.get()
	if len(failed) != 1 || failed[0].RetryIn != 0 || failed[0].Version != 100 || !strings.Contains(failed[0].Error.Error(), "is newer than the recovery version 100") {
		t.Fatalf("failure reported as %+v", failed)
	}
	if len(lines) != 1 || !strings.Contains(lines[0], "Dropped the app state regular recovery snapshot v100") {
		t.Fatalf("ERROR lines: %q", lines)
	}
	if labelVersion(t, cli) != 101 || !reflect.DeepEqual(seen.get(), []string{"old"}) || len(notices.caughtUp()) != 0 {
		t.Fatalf("version=%d seen=%v caughtUp=%d", labelVersion(t, cli), seen.get(), len(notices.caughtUp()))
	}
}

// Disconnect drops a deferred snapshot: it is not adopted into stores the
// application may be closing, and the application is told.
func TestDeferredRecoveryIsDroppedOnDisconnect(t *testing.T) {
	cli, _ := reviewLabelClient(t)
	cli.AppStateBacklogLimit = 1
	notices := watchBacklog(cli)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	release, fetched, seen := holdLabelFetch(t, ctx, cli)
	if !cli.handleAppStateRecovery(ctx, "deferred", labelRecovery(t, cli, 110, "recovered")) {
		t.Fatal("deferred recovery reported as a handler failure")
	}
	cli.Disconnect()
	eventually(t, ctx, "the drop to be reported", func() bool { return len(notices.failed()) == 1 })
	if failed := notices.failed()[0]; failed.RetryIn != 0 || failed.Version != 110 || !errors.Is(failed.Error, ErrNotConnected) {
		t.Fatalf("drop reported as %+v", failed)
	}
	release()
	if err := <-fetched; err != nil {
		t.Fatal(err)
	}
	unfinished, err := cli.PauseAppState(ctx)
	if err != nil || unfinished[appstate.WAPatchRegular] != (AppStateUnfinished{RecoveryDropped: true}) || labelVersion(t, cli) != 101 || !reflect.DeepEqual(seen.get(), []string{"old"}) {
		t.Fatalf("unfinished=%+v err=%v version=%d seen=%v", unfinished, err, labelVersion(t, cli), seen.get())
	}
}
