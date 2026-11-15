package whatsmeow

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/appstate/lthash"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/proto/waSyncdSnapshotRecovery"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
)

var debtStateNames = map[appStateDebtState]string{debtReady: "ready", debtBackoff: "backoff", debtOffline: "offline", debtOnHold: "on hold"}

var debtEventNames = map[appStateDebtEvent]string{
	debtRefused: "refused", debtConnected: "connected", debtDisconnected: "disconnected", debtPaused: "paused",
	debtTimerFired: "timer fired", debtAttemptNoConnection: "attempt: no connection", debtAttemptFailed: "attempt: failed",
	debtRecoveryAdopted: "adopted",
}

// Every cell of the table next to appStateDebtNext, written out again here:
// state x event -> next state, whether the failure count is reset, and the
// effect. Cells that must do nothing say so.
func TestAppStateDebtTable(t *testing.T) {
	type cell struct {
		next   appStateDebtState
		reset  bool
		effect appStateDebtEffect
	}
	same := func(state appStateDebtState) cell { return cell{next: state} }
	table := map[appStateDebtState]map[appStateDebtEvent]cell{
		debtReady: {
			debtRefused:             same(debtReady),
			debtRecoveryAdopted:     same(debtReady),
			debtConnected:           same(debtReady),
			debtDisconnected:        {next: debtOffline, effect: debtAnnounceParked},
			debtPaused:              same(debtReady),
			debtTimerFired:          same(debtReady),
			debtAttemptNoConnection: {next: debtOffline, effect: debtAnnounceParked},
			// debtAttemptFailed: below, it depends on the count and the pause.
		},
		debtBackoff: {
			debtRefused:             same(debtBackoff),
			debtRecoveryAdopted:     same(debtBackoff),
			debtConnected:           same(debtBackoff),
			debtDisconnected:        {next: debtOffline, effect: debtAnnounceParked},
			debtPaused:              same(debtReady),
			debtTimerFired:          same(debtReady),
			debtAttemptNoConnection: same(debtBackoff),
			debtAttemptFailed:       same(debtBackoff),
		},
		debtOffline: {
			debtRefused:             same(debtOffline),
			debtRecoveryAdopted:     same(debtOffline),
			debtConnected:           {next: debtReady, reset: true},
			debtDisconnected:        same(debtOffline),
			debtPaused:              same(debtOffline),
			debtTimerFired:          same(debtOffline),
			debtAttemptNoConnection: same(debtOffline),
			debtAttemptFailed:       same(debtOffline),
		},
		debtOnHold: {
			debtRefused:             {next: debtReady, reset: true},
			debtRecoveryAdopted:     {next: debtReady, reset: true},
			debtConnected:           {next: debtReady, reset: true},
			debtDisconnected:        same(debtOffline),
			debtPaused:              same(debtOnHold),
			debtTimerFired:          same(debtOnHold),
			debtAttemptNoConnection: same(debtOnHold),
			debtAttemptFailed:       same(debtOnHold),
		},
	}
	for state, name := range debtStateNames {
		for event := appStateDebtEvent(0); event < debtEvents; event++ {
			for _, paused := range []bool{false, true} {
				for failures := range appStateCatchUpMaxFailures {
					next, after, effect := appStateDebtNext(state, event, paused, failures)
					what := fmt.Sprintf("%s + %s (paused=%v, failures=%d)", name, debtEventNames[event], paused, failures)
					if state == debtReady && event == debtAttemptFailed {
						// The one cell that counts: the fifth failure gives up,
						// an earlier one backs off, and during a pause it is
						// only counted, as no timer may be armed.
						want, wantEffect := debtBackoff, debtArmRetry
						switch {
						case failures+1 == appStateCatchUpMaxFailures:
							want, wantEffect = debtOnHold, debtAnnounceGaveUp
						case paused:
							want, wantEffect = debtReady, debtNoEffect
						}
						if next != want || after != failures+1 || effect != wantEffect {
							t.Errorf("%s: %s, %d failures, effect %d", what, debtStateNames[next], after, effect)
						}
						continue
					}
					want, ok := table[state][event]
					if !ok {
						t.Fatalf("%s: the table has no cell", what)
					}
					wantFailures := failures
					if want.reset {
						wantFailures = 0
					}
					if next != want.next || after != wantFailures || effect != want.effect {
						t.Errorf("%s: %s, %d failures, effect %d; want %s, %d, %d", what, debtStateNames[next], after, effect, debtStateNames[want.next], wantFailures, want.effect)
					}
					// No cell arms a timer except the one above.
					if effect == debtArmRetry || next == debtBackoff && state != debtBackoff {
						t.Errorf("%s: arms a retry", what)
					}
				}
			}
		}
	}
	// The start rule: only a ready debt, and only with all three conditions.
	for state, name := range debtStateNames {
		for mask := range 8 {
			paused, busy, capacity := mask&1 != 0, mask&2 != 0, mask&4 != 0
			if got, want := appStateDebtStarts(state, paused, busy, capacity), state == debtReady && !paused && !busy && capacity; got != want {
				t.Errorf("start %s paused=%v busy=%v capacity=%v: %v", name, paused, busy, capacity, got)
			}
		}
	}
}

// debtWorld is a publisher driven through its real entry points, with the
// two workers themselves replaced: an attempt or an adoption "runs" until the
// test ends it. Notices are kept instead of delivered, so the test sees which
// ones an event produced.
type debtWorld struct {
	cli  *Client
	slot *appStateBatch // the collection's only backlog slot, while something else holds it
	// adopting is the snapshot the adoption worker is running for.
	adopting *deferredAppStateRecovery
	versions uint64
}

const debtWorldName = appstate.WAPatchRegular

func newDebtWorld() *debtWorld {
	cli := &Client{Log: waLog.Noop, AppStateBacklogLimit: 1}
	p := &cli.appStatePublisher
	p.retryBase = time.Hour
	p.catchUp = func(appstate.WAPatchName, uint64) {}
	p.adopt = func(appstate.WAPatchName, *deferredAppStateRecovery) {}
	p.noticing = true // as if a delivery were running: notices stay queued
	return &debtWorld{cli: cli}
}

type debtView struct {
	owed                                       bool
	state                                      appStateDebtState
	failures                                   int
	timer, offline                             bool
	paused, busy, capacity, disconnected, held bool
	// retained: a deferred recovery snapshot waits. adopting: the worker
	// that is running is its adoption, not an attempt.
	retained, adopting bool
}

func (w *debtWorld) view() debtView {
	p := &w.cli.appStatePublisher
	p.mu.Lock()
	defer p.mu.Unlock()
	v := debtView{paused: p.paused, busy: p.catchingUp[debtWorldName], disconnected: p.disconnected, held: w.slot != nil}
	v.capacity = p.backlogLocked(debtWorldName).Depth < 1
	v.retained = p.recoveries[debtWorldName] != nil
	v.adopting = v.busy && w.adopting != nil
	if debt := p.owed[debtWorldName]; debt != nil {
		v.owed, v.state, v.failures, v.timer, v.offline = true, debt.state, debt.failures, debt.timer != nil, debt.offline
	}
	return v
}

// announced takes the notices an event queued: whether it announced that
// the collection is caught up.
func (w *debtWorld) announced() (caughtUp bool) {
	p := &w.cli.appStatePublisher
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, notice := range p.notices {
		if _, ok := notice.(*events.AppStateCaughtUp); ok {
			caughtUp = true
		}
	}
	p.notices = nil
	return caughtUp
}

// apply runs an event and notes which worker it started, if any: a retained
// snapshot is adopted before an owed fetch is attempted.
func (w *debtWorld) apply(event debtWorldEvent) {
	p := &w.cli.appStatePublisher
	before := w.view()
	event.apply(w)
	p.mu.Lock()
	defer p.mu.Unlock()
	if !before.busy && p.catchingUp[debtWorldName] {
		w.adopting = p.recoveries[debtWorldName]
	}
}

func (w *debtWorld) close() {
	p := &w.cli.appStatePublisher
	p.mu.Lock()
	defer p.mu.Unlock()
	if debt := p.owed[debtWorldName]; debt != nil {
		debt.stopTimer()
	}
}

func (w *debtWorld) endAttempt(err error, staleConnection bool) {
	p := &w.cli.appStatePublisher
	p.mu.Lock()
	connects := p.connects
	p.mu.Unlock()
	if staleConnection {
		connects--
	}
	w.cli.endAppStateCatchUp(debtWorldName, connects, err)
}

func (w *debtWorld) endAdoption(err error) {
	recovery := w.adopting
	w.adopting = nil
	w.cli.endAppStateAdoption(debtWorldName, recovery, err)
}

type debtWorldEvent struct {
	name  string
	can   func(debtView) bool
	apply func(*debtWorld)
}

func attempting(v debtView) bool { return v.busy && !v.adopting }

var debtWorldEvents = []debtWorldEvent{
	{"refused", func(v debtView) bool { return v.paused || !v.capacity }, func(w *debtWorld) {
		_ = w.cli.claimAppStateBatch(&appStateBatch{name: debtWorldName, source: "notification", version: 7})
	}},
	{"recovery refused", func(v debtView) bool { return v.paused || !v.capacity }, func(w *debtWorld) {
		w.versions++
		_, _ = w.cli.adoptAppStateRecovery(context.Background(), &deferredAppStateRecovery{
			reqID: "walk", version: w.versions,
			snapshot: &waSyncdSnapshotRecovery.SyncdSnapshotRecovery{CollectionName: proto.String(string(debtWorldName))},
		}, false)
	}},
	{"capacity taken", func(v debtView) bool { return !v.held && !v.paused && v.capacity }, func(w *debtWorld) {
		w.slot = &appStateBatch{name: debtWorldName}
		if err := w.cli.claimAppStateBatch(w.slot); err != nil {
			panic(err)
		}
	}},
	{"capacity back", func(v debtView) bool { return v.held }, func(w *debtWorld) {
		w.cli.finishAppStateBatch(w.slot, nil)
		w.slot = nil
	}},
	{"connected", func(debtView) bool { return true }, func(w *debtWorld) { w.cli.resumeAppStateCatchUps() }},
	{"socket lost", func(debtView) bool { return true }, func(w *debtWorld) { w.cli.parkAppStateDebts() }},
	{"Disconnect", func(debtView) bool { return true }, func(w *debtWorld) { w.cli.parkAppStateCatchUps() }},
	{"paused", func(v debtView) bool { return !v.paused }, func(w *debtWorld) {
		done, cancel := context.WithCancel(context.Background())
		cancel()
		_, _ = w.cli.PauseAppState(done)
	}},
	{"resumed", func(v debtView) bool { return v.paused }, func(w *debtWorld) { w.cli.ResumeAppState() }},
	{"timer fired", func(v debtView) bool { return v.timer }, func(w *debtWorld) {
		p := &w.cli.appStatePublisher
		p.mu.Lock()
		debt := p.owed[debtWorldName]
		retry := debt.retry
		p.mu.Unlock()
		w.cli.retryAppStateCatchUp(debtWorldName, debt, retry)
	}},
	{"stale timer fired", func(v debtView) bool { return v.owed }, func(w *debtWorld) {
		p := &w.cli.appStatePublisher
		p.mu.Lock()
		debt := p.owed[debtWorldName]
		retry := debt.retry
		p.mu.Unlock()
		w.cli.retryAppStateCatchUp(debtWorldName, debt, retry-1)
	}},
	{"attempt settled it", attempting, func(w *debtWorld) {
		w.cli.settleAppStateCatchUp(debtWorldName, w.cli.appStateCatchUpMark())
		w.endAttempt(nil, false)
	}},
	{"attempt ended, refused meanwhile", attempting, func(w *debtWorld) { w.endAttempt(nil, false) }},
	{"attempt refused again", attempting, func(w *debtWorld) {
		w.endAttempt(&AppStateBacklogError{Name: debtWorldName}, false)
	}},
	{"attempt: no connection", attempting, func(w *debtWorld) { w.endAttempt(ErrNotConnected, false) }},
	{"attempt: no connection, connected meanwhile", attempting, func(w *debtWorld) {
		w.endAttempt(ErrIQDisconnected, true)
	}},
	{"attempt: failed", attempting, func(w *debtWorld) { w.endAttempt(errors.New("store failure"), false) }},
	{"settled by another fetch", func(v debtView) bool { return v.owed }, func(w *debtWorld) {
		w.cli.settleAppStateCatchUp(debtWorldName, w.cli.appStateCatchUpMark())
	}},
	{"adoption succeeded", func(v debtView) bool { return v.adopting }, func(w *debtWorld) { w.endAdoption(nil) }},
	{"adoption refused again", func(v debtView) bool { return v.adopting }, func(w *debtWorld) {
		w.endAdoption(&AppStateBacklogError{Name: debtWorldName})
	}},
	{"adoption failed", func(v debtView) bool { return v.adopting }, func(w *debtWorld) {
		w.endAdoption(errors.New("superseded"))
	}},
	{"snapshot adopted on arrival", func(v debtView) bool { return !v.paused && v.capacity }, func(w *debtWorld) {
		p := &w.cli.appStatePublisher
		p.mu.Lock()
		w.cli.appStateRecoveryAdoptedLocked(debtWorldName, 0)
		p.mu.Unlock()
	}},
}

func debtWorldEventNamed(name string) debtWorldEvent {
	for _, event := range debtWorldEvents {
		if event.name == name {
			return event
		}
	}
	panic(name)
}

func replayDebtWorld(path []int) *debtWorld {
	w := newDebtWorld()
	for _, event := range path {
		w.apply(debtWorldEvents[event])
	}
	return w
}

// The invariants of what a collection is owed, checked in every state the
// publisher can reach through its entry points, after every event that can
// happen there:
//
//  1. A debt that exists is being paid, or waits for a named event that the
//     client signals, and signalling those events gets it paid. The same
//     holds for a retained recovery snapshot and its adoption, and an
//     adoption gets the fetch that is owed besides it paid as well.
//  2. While the client is disconnected or app state is paused, no timer is
//     armed, no worker starts, and no snapshot stays retained through a
//     pause or a Disconnect.
//  3. Nothing announces that a collection is caught up while a fetch is
//     still owed to it.
//
// The walk calls the publisher's entry points directly. That the client's
// own code calls them, when a batch is delivered, on login, when the socket
// is lost and when a snapshot is adopted, is shown by
// TestAppStateDebtEventsComeFromTheClient.
func TestAppStateDebtInvariantsInEveryReachableState(t *testing.T) {
	names := func(path []int) []string {
		out := make([]string, len(path))
		for i, event := range path {
			out[i] = debtWorldEvents[event].name
		}
		return out
	}
	check := func(path []int, before, v debtView, caughtUp bool) {
		t.Helper()
		where := fmt.Sprintf("after %q: %+v", names(path), v)
		last := debtWorldEvents[path[len(path)-1]].name
		if caughtUp && v.owed {
			t.Fatalf("caught up was announced while a fetch is owed %s", where)
		}
		if v.retained && (v.paused || last == "Disconnect") {
			t.Fatalf("a snapshot stayed retained through a pause or Disconnect %s", where)
		}
		if v.retained && !v.paused && !v.busy && v.capacity {
			t.Fatalf("a retained snapshot is not being adopted, with nothing to wait for, %s", where)
		}
		if v.busy && !before.busy && v.paused {
			t.Fatalf("a worker started while paused %s", where)
		}
		if !v.owed {
			return
		}
		if (last == "adoption succeeded" || last == "snapshot adopted on arrival") && v.state == debtOnHold {
			t.Fatalf("a snapshot was adopted and the fetch owed besides it stays on hold %s", where)
		}
		if v.timer != (v.state == debtBackoff) || v.offline != (v.state == debtOffline || v.state == debtOnHold) {
			t.Fatalf("fields disagree with the state %s", where)
		}
		if !v.retained && appStateDebtStarts(v.state, v.paused, v.busy, v.capacity) {
			t.Fatalf("a debt is ready, with nothing running and nothing to wait for, %s", where)
		}
		if (v.paused || v.disconnected) && v.timer {
			t.Fatalf("a timer is armed while paused or disconnected %s", where)
		}
		if v.disconnected && v.state != debtOffline {
			t.Fatalf("a debt does not wait for the connection while disconnected %s", where)
		}
		if v.busy && !before.busy && !v.adopting && v.disconnected {
			t.Fatalf("an attempt started while disconnected %s", where)
		}
	}
	// What each wait is for. Signalled in this order, they must get what is
	// owed paid, whatever state it is in: a retained snapshot is adopted,
	// and once it is, the owed fetch is attempted.
	wake := func(path []int) {
		t.Helper()
		w := replayDebtWorld(path)
		defer w.close()
		for _, signal := range []string{"timer fired", "connected", "resumed", "capacity back"} {
			if event := debtWorldEventNamed(signal); event.can(w.view()) {
				w.apply(event)
			}
		}
		v := w.view()
		if (v.owed || v.retained) && !v.busy {
			t.Fatalf("after its timer, a connection, the resume and capacity, nothing is being paid: path %q, %+v", names(path), v)
		}
		if v.adopting {
			w.apply(debtWorldEventNamed("adoption succeeded"))
			if caughtUp := w.announced(); caughtUp == w.view().owed {
				t.Fatalf("after the adoption, caught up announced=%v with a fetch owed=%v: path %q", caughtUp, w.view().owed, names(path))
			}
			if v = w.view(); v.owed && !attempting(v) {
				t.Fatalf("a snapshot was adopted and the fetch owed besides it is not attempted: path %q, %+v", names(path), v)
			}
		}
	}

	seen := map[debtView][]int{}
	states := map[appStateDebtState]bool{}
	var retainedOnHold bool
	queue := [][]int{{}}
	root := newDebtWorld()
	seen[root.view()] = []int{}
	for len(queue) > 0 {
		path := queue[0]
		queue = queue[1:]
		w := replayDebtWorld(path)
		from := w.view()
		w.close()
		for index, event := range debtWorldEvents {
			if !event.can(from) {
				continue
			}
			next := append(append([]int(nil), path...), index)
			w := replayDebtWorld(path)
			w.announced()
			w.apply(event)
			v, caughtUp := w.view(), w.announced()
			w.close()
			check(next, from, v, caughtUp)
			if _, known := seen[v]; known {
				continue
			}
			seen[v] = next
			if v.owed {
				states[v.state] = true
				retainedOnHold = retainedOnHold || v.retained && v.state == debtOnHold
			}
			wake(next)
			queue = append(queue, next)
		}
	}
	if len(states) != len(debtStateNames) || !retainedOnHold || len(seen) < 200 {
		t.Fatalf("the walk reached %d states of the publisher, the debt states %v, a snapshot retained beside a debt on hold: %v", len(seen), states, retainedOnHold)
	}
	t.Logf("checked %d reachable states of the publisher", len(seen))
}

// The walk above calls the publisher's entry points. Here the client's own
// code calls them: each event that the client produces itself is produced
// once by the code that produces it, on a real client with a real store and
// peer, and the debt must move as the table says.
func TestAppStateDebtEventsComeFromTheClient(t *testing.T) {
	parked := func(t *testing.T, ctx context.Context) (*Client, *fakeSyncdServer, *failingVersionStore) {
		t.Helper()
		cli, server, _, slot := owedLabels(t, ctx, 102)
		cli.EnableAutoReconnect = false
		failing := &failingVersionStore{AppStateStore: cli.Store.AppState}
		failing.failures.Store(1000)
		cli.Store.AppState = failing
		cli.appStatePublisher.retryBase = time.Millisecond
		cli.finishAppStateBatch(slot, nil)
		eventually(t, ctx, "the debt to be put on hold", func() bool {
			debt, _ := cli.appStatePublisher.debt(appstate.WAPatchRegular)
			return debt.state == debtOnHold
		})
		if err := cli.WaitForAppStateEvents(ctx); err != nil {
			t.Fatal(err)
		}
		failing.failures.Store(0)
		return cli, server, failing
	}
	paid := func(t *testing.T, ctx context.Context, cli *Client, version uint64) {
		t.Helper()
		if err := cli.WaitForAppStateEvents(ctx); err != nil {
			t.Fatal(err)
		}
		if debt, owed := cli.appStatePublisher.debt(appstate.WAPatchRegular); owed || labelVersion(t, cli) != version {
			t.Fatalf("owed=%v %+v, version %d, want the debt paid at v%d", owed, debt, labelVersion(t, cli), version)
		}
	}

	t.Run("capacity: a held batch is delivered", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		cli, server := reviewLabelClient(t)
		cli.AppStateBacklogLimit = 1
		release, fetched, _ := holdLabelFetch(t, ctx, cli)
		appendLabelPatch(t, cli, server, "pending")
		cli.handleAppStateNotification(ctx, labelNotification(102))
		if debt, owed := cli.appStatePublisher.debt(appstate.WAPatchRegular); !owed || debt.state != debtReady {
			t.Fatalf("debt behind a held handler: owed=%v %+v", owed, debt)
		}
		release()
		if err := <-fetched; err != nil {
			t.Fatal(err)
		}
		paid(t, ctx, cli, 102)
	})
	t.Run("connected: the server confirms the login", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		cli, server, _ := parked(t, ctx)
		reconnected := attachFakeServer(t, cli, appstate.WAPatchRegular)
		server.mu.Lock()
		reconnected.version, reconnected.patches = server.version, server.patches
		server.mu.Unlock()
		// The handler the client registered for the server's success node.
		// The peer answers no other request, so those get little time.
		login, done := context.WithTimeout(ctx, 200*time.Millisecond)
		defer done()
		cli.nodeHandlers["success"](login, &waBinary.Node{Tag: "success", Attrs: waBinary.Attrs{"t": time.Now().Unix()}})
		eventually(t, ctx, "the attempt after the login", func() bool {
			_, owed := cli.appStatePublisher.debt(appstate.WAPatchRegular)
			return !owed
		})
		paid(t, ctx, cli, 102)
	})
	t.Run("disconnected: the socket is lost", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		cli, _, failing := parked(t, ctx)
		attempts := failing.attempts.Load()
		cli.socketLock.RLock()
		lost := cli.socket
		cli.socketLock.RUnlock()
		// The callback the client gives its socket.
		cli.onDisconnect(ctx, lost, true)
		// On hold, a new refusal would have renewed it. Offline, it does not.
		slot := &appStateBatch{name: appstate.WAPatchRegular}
		if err := cli.claimAppStateBatch(slot); err != nil {
			t.Fatal(err)
		}
		cli.handleAppStateNotification(ctx, labelNotification(103))
		cli.finishAppStateBatch(slot, nil)
		if err := cli.WaitForAppStateEvents(ctx); err != nil {
			t.Fatal(err)
		}
		if debt, owed := cli.appStatePublisher.debt(appstate.WAPatchRegular); !owed || debt.state != debtOffline || failing.attempts.Load() != attempts {
			t.Fatalf("after the socket was lost: owed=%v %+v, %d more attempts", owed, debt, failing.attempts.Load()-attempts)
		}
	})
	for _, retained := range []bool{false, true} {
		name := "adopted: a recovery snapshot arrives"
		if retained {
			name = "adopted: a retained recovery snapshot is adopted after the drain"
		}
		t.Run(name, func(t *testing.T) { adoptedSnapshotRenewsDebt(t, retained, parked, paid) })
	}
}

func adoptedSnapshotRenewsDebt(
	t *testing.T, retained bool,
	parked func(*testing.T, context.Context) (*Client, *fakeSyncdServer, *failingVersionStore),
	paid func(*testing.T, context.Context, *Client, uint64),
) {
	{
		ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
		defer cancel()
		cli, server, _ := parked(t, ctx)
		notices := watchBacklog(cli)
		// What the server has on top of the snapshot the primary sends.
		mac := make([]byte, 32)
		mac[0] = 110
		state := appstate.HashState{Version: 110}
		lthash.WAPatchIntegrity.SubtractThenAddInPlace(state.Hash[:], nil, [][]byte{mac})
		key, err := cli.Store.AppStateKeys.GetLatestAppStateSyncKeyID(ctx)
		if err != nil {
			t.Fatal(err)
		}
		raw, err := cli.appStateProc.EncodePatch(ctx, key, state, appstate.BuildLabelEdit("8", "after the snapshot", 1, false))
		if err != nil {
			t.Fatal(err)
		}
		server.mu.Lock()
		server.version = 110
		server.appendPatch(raw)
		server.mu.Unlock()
		// With the backlog full the snapshot is retained, and adopted by
		// the collection's worker when the capacity is back.
		var slot *appStateBatch
		if retained {
			slot = &appStateBatch{name: appstate.WAPatchRegular}
			if err := cli.claimAppStateBatch(slot); err != nil {
				t.Fatal(err)
			}
		}
		// The peer data operation response, through the protocol message path.
		if !cli.handleAppStateRecovery(ctx, "repair", labelRecovery(t, cli, 110, "recovered")) {
			t.Fatal("recovery was not handled")
		}
		if retained {
			if debt, _ := cli.appStatePublisher.debt(appstate.WAPatchRegular); debt.state != debtOnHold || labelVersion(t, cli) != 101 {
				t.Fatalf("before the adoption: %+v at v%d", debt, labelVersion(t, cli))
			}
			cli.finishAppStateBatch(slot, nil)
		}
		paid(t, ctx, cli, 111)
		// Announced once, and only by the fetch that settled the debt.
		if caughtUp := notices.caughtUp(); len(caughtUp) != 1 || caughtUp[0].Version != 102 {
			t.Fatalf("caught up announced as %+v", caughtUp)
		}
	}
}
