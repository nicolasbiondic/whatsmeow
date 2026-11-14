package whatsmeow

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/appstate"
	waLog "go.mau.fi/whatsmeow/util/log"
)

var debtStateNames = map[appStateDebtState]string{debtReady: "ready", debtBackoff: "backoff", debtOffline: "offline", debtOnHold: "on hold"}

var debtEventNames = map[appStateDebtEvent]string{
	debtRefused: "refused", debtConnected: "connected", debtDisconnected: "disconnected", debtPaused: "paused",
	debtTimerFired: "timer fired", debtAttemptNoConnection: "attempt: no connection", debtAttemptFailed: "attempt: failed",
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
			debtConnected:           same(debtReady),
			debtDisconnected:        {next: debtOffline, effect: debtAnnounceParked},
			debtPaused:              same(debtReady),
			debtTimerFired:          same(debtReady),
			debtAttemptNoConnection: {next: debtOffline, effect: debtAnnounceParked},
			// debtAttemptFailed: below, it depends on the count and the pause.
		},
		debtBackoff: {
			debtRefused:             same(debtBackoff),
			debtConnected:           same(debtBackoff),
			debtDisconnected:        {next: debtOffline, effect: debtAnnounceParked},
			debtPaused:              same(debtReady),
			debtTimerFired:          same(debtReady),
			debtAttemptNoConnection: same(debtBackoff),
			debtAttemptFailed:       same(debtBackoff),
		},
		debtOffline: {
			debtRefused:             same(debtOffline),
			debtConnected:           {next: debtReady, reset: true},
			debtDisconnected:        same(debtOffline),
			debtPaused:              same(debtOffline),
			debtTimerFired:          same(debtOffline),
			debtAttemptNoConnection: same(debtOffline),
			debtAttemptFailed:       same(debtOffline),
		},
		debtOnHold: {
			debtRefused:             {next: debtReady, reset: true},
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
// attempt itself replaced: it "runs" until the test ends it.
type debtWorld struct {
	cli  *Client
	slot *appStateBatch // the collection's only backlog slot, while something else holds it
}

const debtWorldName = appstate.WAPatchRegular

func newDebtWorld() *debtWorld {
	cli := &Client{Log: waLog.Noop, AppStateBacklogLimit: 1}
	cli.appStatePublisher.retryBase = time.Hour
	cli.appStatePublisher.catchUp = func(appstate.WAPatchName, uint64) {}
	return &debtWorld{cli: cli}
}

type debtView struct {
	owed                                       bool
	state                                      appStateDebtState
	failures                                   int
	timer, offline                             bool
	paused, busy, capacity, disconnected, held bool
}

func (w *debtWorld) view() debtView {
	p := &w.cli.appStatePublisher
	p.mu.Lock()
	defer p.mu.Unlock()
	v := debtView{paused: p.paused, busy: p.catchingUp[debtWorldName], disconnected: p.disconnected, held: w.slot != nil}
	v.capacity = p.backlogLocked(debtWorldName).Depth < 1
	if debt := p.owed[debtWorldName]; debt != nil {
		v.owed, v.state, v.failures, v.timer, v.offline = true, debt.state, debt.failures, debt.timer != nil, debt.offline
	}
	return v
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

type debtWorldEvent struct {
	name  string
	can   func(debtView) bool
	apply func(*debtWorld)
}

var debtWorldEvents = []debtWorldEvent{
	{"refused", func(v debtView) bool { return v.paused || !v.capacity }, func(w *debtWorld) {
		_ = w.cli.claimAppStateBatch(&appStateBatch{name: debtWorldName, source: "notification", version: 7})
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
	{"attempt settled it", func(v debtView) bool { return v.busy }, func(w *debtWorld) {
		w.cli.settleAppStateCatchUp(debtWorldName, w.cli.appStateCatchUpMark())
		w.endAttempt(nil, false)
	}},
	{"attempt ended, refused meanwhile", func(v debtView) bool { return v.busy }, func(w *debtWorld) { w.endAttempt(nil, false) }},
	{"attempt refused again", func(v debtView) bool { return v.busy }, func(w *debtWorld) {
		w.endAttempt(&AppStateBacklogError{Name: debtWorldName}, false)
	}},
	{"attempt: no connection", func(v debtView) bool { return v.busy }, func(w *debtWorld) { w.endAttempt(ErrNotConnected, false) }},
	{"attempt: no connection, connected meanwhile", func(v debtView) bool { return v.busy }, func(w *debtWorld) {
		w.endAttempt(ErrIQDisconnected, true)
	}},
	{"attempt: failed", func(v debtView) bool { return v.busy }, func(w *debtWorld) { w.endAttempt(errors.New("store failure"), false) }},
	{"settled by another fetch", func(v debtView) bool { return v.owed }, func(w *debtWorld) {
		w.cli.settleAppStateCatchUp(debtWorldName, w.cli.appStateCatchUpMark())
	}},
}

func replayDebtWorld(path []int) *debtWorld {
	w := newDebtWorld()
	for _, event := range path {
		debtWorldEvents[event].apply(w)
	}
	return w
}

// The two invariants of an owed fetch, checked in every state the publisher
// can reach through its entry points, after every event that can happen
// there:
//
//  1. A debt that exists is being paid, or waits for a named event that the
//     client signals, and signalling those events gets it paid.
//  2. While the client is disconnected or app state is paused, no timer is
//     armed and no attempt starts.
func TestAppStateDebtInvariantsInEveryReachableState(t *testing.T) {
	check := func(path []int, before, v debtView) {
		t.Helper()
		where := func() string {
			names := make([]string, len(path))
			for i, event := range path {
				names[i] = debtWorldEvents[event].name
			}
			return fmt.Sprintf("after %q: %+v", names, v)
		}
		if !v.owed {
			return
		}
		if v.timer != (v.state == debtBackoff) || v.offline != (v.state == debtOffline || v.state == debtOnHold) {
			t.Fatalf("fields disagree with the state %s", where())
		}
		if appStateDebtStarts(v.state, v.paused, v.busy, v.capacity) {
			t.Fatalf("a debt is ready, with nothing running and nothing to wait for, %s", where())
		}
		if (v.paused || v.disconnected) && v.timer {
			t.Fatalf("a timer is armed while paused or disconnected %s", where())
		}
		if v.disconnected && v.state != debtOffline {
			t.Fatalf("a debt does not wait for the connection while disconnected %s", where())
		}
		if v.busy && !before.busy && (v.paused || v.disconnected) {
			t.Fatalf("an attempt started while paused or disconnected %s", where())
		}
	}
	// What each wait is for. Signalled in this order, they must get the
	// debt paid, whatever state it is in.
	wake := func(path []int) {
		t.Helper()
		w := replayDebtWorld(path)
		defer w.close()
		for _, signal := range []string{"timer fired", "connected", "resumed", "capacity back"} {
			for _, event := range debtWorldEvents {
				if event.name == signal && event.can(w.view()) {
					event.apply(w)
				}
			}
		}
		if v := w.view(); v.owed && !v.busy {
			t.Fatalf("after its timer, a connection, the resume and capacity, the debt is still not being paid: path %v, %+v", path, v)
		}
	}

	seen := map[debtView][]int{}
	states := map[appStateDebtState]bool{}
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
			w := replayDebtWorld(next)
			v := w.view()
			w.close()
			check(next, from, v)
			if _, known := seen[v]; known {
				continue
			}
			seen[v] = next
			if v.owed {
				states[v.state] = true
			}
			wake(next)
			queue = append(queue, next)
		}
	}
	if len(states) != len(debtStateNames) || len(seen) < 60 {
		t.Fatalf("the walk reached %d states of the publisher and the debt states %v", len(seen), states)
	}
	t.Logf("checked %d reachable states of the publisher", len(seen))
}
