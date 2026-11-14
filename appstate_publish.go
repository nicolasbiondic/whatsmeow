// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"slices"
	"sync"
	"time"

	"go.mau.fi/whatsmeow/appstate"
)

// DefaultAppStateBacklogLimit is used when Client.AppStateBacklogLimit is zero.
const DefaultAppStateBacklogLimit = 64

// ErrAppStateBacklog is matched by the error that app state fetches, sends
// and recovery adoptions fail with while too many committed event batches of
// their collection are still undelivered. See AppStateBacklogError.
var ErrAppStateBacklog = errors.New("app state events are backed up")

// ErrAppStatePaused is what app state fetches and sends fail with between
// Client.PauseAppState and Client.ResumeAppState. Nothing was sent or stored.
var ErrAppStatePaused = errors.New("app state is paused")

// AppStateBacklogError reports that nothing was sent or stored, because the
// collection's event backlog is at Client.AppStateBacklogLimit. It is
// transient. A fetch it refused is owed: the client does it by itself once
// the event handlers have caught up (events.AppStateDeferred).
type AppStateBacklogError struct {
	Name appstate.WAPatchName
	AppStateBacklog
}

func (e *AppStateBacklogError) Error() string {
	return fmt.Sprintf("%v: %s has %d undelivered batches, the oldest for %s", ErrAppStateBacklog, e.Name, e.Depth, e.OldestAge.Round(time.Millisecond))
}

func (e *AppStateBacklogError) Is(target error) bool {
	return target == ErrAppStateBacklog
}

// AppStateBacklog describes the event batches of one collection that the
// event handlers have not finished yet.
type AppStateBacklog struct {
	// Depth counts the batch being dispatched, those queued behind it, and
	// the work that was admitted and has not committed or ended yet.
	Depth int
	// OldestAge is the time since the oldest committed one was committed.
	OldestAge time.Duration
}

// appStateBatch is the event batch of one fetch, conflict response, send or
// recovery. It takes its place in the collection's publication queue when its
// first write commits, while the collection lock is still held, so the queue
// order is the commit order regardless of which goroutine publishes first.
type appStateBatch struct {
	name    appstate.WAPatchName
	source  string // what asked for it, for the diagnostics of a refusal
	version uint64 // the version a server notification announced, if one did
	quiet   bool   // a catch-up attempt: a refusal is already known and owed
	// recovery is the snapshot this batch adopts. Refused, it is kept and
	// adopted later instead of leaving a fetch owed.
	recovery *deferredAppStateRecovery
	claimed  bool // holds backlog capacity, has no queue place yet
	queued   bool
	ready    bool
	since    time.Time
	publish  func()
}

type appStateBatchKey struct{}

// appStatePublisher publishes the batches of each collection in commit order.
// Nobody waits for another batch: a goroutine that finishes a batch while an
// earlier one is still being published leaves it queued, and the goroutine
// publishing that earlier batch delivers it next. A handler may therefore
// fetch the collection it is being called for without deadlocking, and no
// lock is held while handlers run.
//
// Committed batches are never dropped and producers never wait, so the queue
// is bounded where work begins: a batch claims capacity before its first step
// that stores or sends anything (claimAppStateBatch), and is refused there.
// What a refusal skipped is made up for later (appstate_catchup.go).
type appStatePublisher struct {
	mu       sync.Mutex
	pending  map[appstate.WAPatchName][]*appStateBatch
	draining map[appstate.WAPatchName]bool
	// publishing holds the commit time of the batch that is in the handlers.
	publishing map[appstate.WAPatchName]time.Time
	// claims counts the batches that hold capacity without a queue place.
	claims map[appstate.WAPatchName]int

	// Catch-up of refused work, see appstate_catchup.go.
	owed       map[appstate.WAPatchName]*appStateDebt
	recoveries map[appstate.WAPatchName]*deferredAppStateRecovery
	catchingUp map[appstate.WAPatchName]bool
	refusals   uint64
	connects   uint64
	retryBase  time.Duration
	notices    []any
	noticing   bool

	// paused fences new work, see PauseAppState. dropped names the
	// collections whose recovery snapshot was given up since the last resume.
	paused  bool
	dropped map[appstate.WAPatchName]bool

	// changed is closed whenever something a waiter looks at has changed.
	changed chan struct{}
}

func (p *appStatePublisher) initLocked() {
	if p.pending != nil {
		return
	}
	p.pending = make(map[appstate.WAPatchName][]*appStateBatch)
	p.draining = make(map[appstate.WAPatchName]bool)
	p.publishing = make(map[appstate.WAPatchName]time.Time)
	p.claims = make(map[appstate.WAPatchName]int)
	p.owed = make(map[appstate.WAPatchName]*appStateDebt)
	p.recoveries = make(map[appstate.WAPatchName]*deferredAppStateRecovery)
	p.catchingUp = make(map[appstate.WAPatchName]bool)
	p.dropped = make(map[appstate.WAPatchName]bool)
}

func (p *appStatePublisher) backlogLocked(name appstate.WAPatchName) AppStateBacklog {
	queue := p.pending[name]
	backlog := AppStateBacklog{Depth: len(queue) + p.claims[name]}
	oldest, committed := p.publishing[name]
	if committed {
		backlog.Depth++
	} else if len(queue) > 0 {
		oldest, committed = queue[0].since, true
	}
	if committed {
		backlog.OldestAge = time.Since(oldest)
	}
	return backlog
}

// AppStateBacklog returns how many event batches of the collection the event
// handlers have not finished, and how old the oldest committed one is.
func (cli *Client) AppStateBacklog(name appstate.WAPatchName) AppStateBacklog {
	p := &cli.appStatePublisher
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.backlogLocked(name)
}

func (cli *Client) appStatePaused() bool {
	p := &cli.appStatePublisher
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.paused
}

func (cli *Client) appStateBacklogLimit() int {
	if cli.AppStateBacklogLimit == 0 {
		return DefaultAppStateBacklogLimit
	}
	return cli.AppStateBacklogLimit
}

// claimAppStateBatch takes backlog capacity for the batch, or refuses it
// with an AppStateBacklogError when the collection is at its limit, or with
// ErrAppStatePaused while app state is paused. The check and the claim are
// one step, so concurrent producers cannot exceed the limit together. It
// never waits: a handler whose own batch is the backlog gets the error
// instead of a deadlock.
//
// A batch claims before its first step that stores or sends anything, so a
// refusal has changed nothing. The claim becomes the batch's queue place when
// it commits, and is given back when the batch ends with nothing to deliver.
// What a refusal skipped is remembered: see deferAppStateLocked.
func (cli *Client) claimAppStateBatch(batch *appStateBatch) error {
	limit := cli.appStateBacklogLimit()
	p := &cli.appStatePublisher
	p.mu.Lock()
	if batch.claimed || batch.queued || batch.ready {
		p.mu.Unlock()
		return nil
	}
	backlog := p.backlogLocked(batch.name)
	if !p.paused && (limit <= 0 || backlog.Depth < limit) {
		p.initLocked()
		batch.claimed = true
		p.claims[batch.name]++
		p.mu.Unlock()
		return nil
	}
	paused := p.paused
	version := cli.deferAppStateLocked(batch, backlog)
	p.mu.Unlock()
	if paused {
		// Not an alarm: whoever paused is told what is left (PauseAppState).
		return fmt.Errorf("%w: %s was not synced (%s)", ErrAppStatePaused, batch.name, batch.source)
	}
	if !batch.quiet {
		// ERROR, not WARN: applications commonly log this client at ERROR only.
		cli.Log.Errorf("App state %s was not synced (%s): %d event batches are undelivered, the oldest for %s; notified version %d. It is made up for when the handlers catch up",
			batch.name, batch.source, backlog.Depth, backlog.OldestAge.Round(time.Millisecond), version)
	}
	return &AppStateBacklogError{Name: batch.name, AppStateBacklog: backlog}
}

// claimAppState claims for the batch that ctx carries for this collection.
// Callers without one apply their patches outside the publication order.
func (cli *Client) claimAppState(ctx context.Context, name appstate.WAPatchName) error {
	if batch, ok := ctx.Value(appStateBatchKey{}).(*appStateBatch); ok && batch.name == name {
		return cli.claimAppStateBatch(batch)
	}
	return nil
}

func (p *appStatePublisher) unclaimLocked(batch *appStateBatch) {
	batch.claimed = false
	if p.claims[batch.name]--; p.claims[batch.name] <= 0 {
		delete(p.claims, batch.name)
	}
}

// WaitForAppStateEvents blocks until the event handlers have finished every
// committed app state batch, or until ctx ends. It also waits for app state
// work that is admitted and still running, including a catch-up attempt, but
// not for a catch-up that is waiting for capacity, a connection or a retry.
//
// It does not keep new work from being admitted meanwhile, so its result
// says little about what follows. To shut down, use PauseAppState.
// Do not call it from an event handler, which would wait for itself.
func (cli *Client) WaitForAppStateEvents(ctx context.Context) error {
	p := &cli.appStatePublisher
	for {
		p.mu.Lock()
		busy := len(p.pending) + len(p.draining) + len(p.claims) + len(p.catchingUp) + len(p.notices)
		if p.noticing {
			busy++
		}
		if busy > 0 && p.changed == nil {
			p.changed = make(chan struct{})
		}
		changed := p.changed
		p.mu.Unlock()
		if busy == 0 {
			return nil
		}
		select {
		case <-changed:
		case <-ctx.Done():
			return fmt.Errorf("app state events are still being delivered: %w", ctx.Err())
		}
	}
}

// AppStateUnfinished is what PauseAppState found unfinished for a collection.
type AppStateUnfinished struct {
	// Undelivered counts the event batches that are committed or still
	// running and that the event handlers have not finished.
	Undelivered int
	// Owed is set when a fetch that was refused, by the backlog or by the
	// pause, has not been made up for. The server still has those changes.
	Owed bool
	// RecoveryDropped is set when a recovery snapshot that was deferred, or
	// that arrived during the pause, was dropped without being adopted.
	RecoveryDropped bool
}

// PauseAppState fences app state work and waits for what is under way.
//
// From here until ResumeAppState, fetches, sends and recovery adoptions fail
// with ErrAppStatePaused before they send or store anything, without the
// ERROR log and the events.AppStateDeferred of a backlog refusal. A refused
// fetch is still owed, and made up for after the resume. No catch-up attempt
// starts, pending retries are cancelled, and recovery snapshots that were
// deferred are dropped.
//
// It then waits, like WaitForAppStateEvents, until the event handlers have
// finished everything that was committed and the admitted work has ended, or
// until ctx ends. It returns what is unfinished per collection: empty when
// everything was delivered and nothing is owed. The error is ctx's when the
// wait ended first; app state stays paused either way.
//
// Call it, with the event handlers still registered, before removing them or
// closing what they write to. Disconnect neither waits for committed batches
// nor drops them. Do not call it from an event handler.
func (cli *Client) PauseAppState(ctx context.Context) (map[appstate.WAPatchName]AppStateUnfinished, error) {
	p := &cli.appStatePublisher
	p.mu.Lock()
	p.initLocked()
	p.paused = true
	for _, debt := range p.owed {
		debt.stopTimer()
	}
	for name, recovery := range p.recoveries {
		cli.dropAppStateRecoveryLocked(name, recovery, ErrAppStatePaused)
	}
	p.mu.Unlock()
	err := cli.WaitForAppStateEvents(ctx)
	p.mu.Lock()
	defer p.mu.Unlock()
	unfinished := make(map[appstate.WAPatchName]AppStateUnfinished)
	note := func(name appstate.WAPatchName) {
		unfinished[name] = AppStateUnfinished{Undelivered: p.backlogLocked(name).Depth, Owed: p.owed[name] != nil, RecoveryDropped: p.dropped[name]}
	}
	for name := range p.pending {
		note(name)
	}
	for name := range p.publishing {
		note(name)
	}
	for name := range p.claims {
		note(name)
	}
	for name := range p.owed {
		note(name)
	}
	for name := range p.dropped {
		note(name)
	}
	return unfinished, err
}

// ResumeAppState ends PauseAppState: work is admitted again, and what was
// refused meanwhile is fetched.
func (cli *Client) ResumeAppState() {
	p := &cli.appStatePublisher
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.paused {
		return
	}
	p.paused = false
	clear(p.dropped)
	for name := range p.owed {
		cli.resumeAppStateCatchUpLocked(name)
	}
}

func newAppStateBatch(ctx context.Context, name appstate.WAPatchName) (context.Context, *appStateBatch) {
	batch := &appStateBatch{name: name}
	return context.WithValue(ctx, appStateBatchKey{}, batch), batch
}

// noteAppStateCommit is the processor's OnCommit hook. It runs under the
// collection lock, which is what makes the queue order the commit order.
func (cli *Client) noteAppStateCommit(ctx context.Context, name appstate.WAPatchName) {
	if batch, ok := ctx.Value(appStateBatchKey{}).(*appStateBatch); ok && batch.name == name {
		cli.appStatePublisher.reserve(batch)
	}
}

// reserve gives a committed batch its queue place. A batch that never
// claimed is queued all the same: what is committed is never dropped.
func (p *appStatePublisher) reserve(batch *appStateBatch) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.enqueueLocked(batch)
}

func (p *appStatePublisher) enqueueLocked(batch *appStateBatch) {
	if batch.queued {
		return
	}
	p.initLocked()
	if batch.claimed {
		p.unclaimLocked(batch)
	}
	batch.queued, batch.since = true, time.Now()
	p.pending[batch.name] = append(p.pending[batch.name], batch)
}

func (p *appStatePublisher) headReadyLocked(name appstate.WAPatchName) bool {
	queue := p.pending[name]
	return len(queue) > 0 && queue[0].ready
}

// finishAppStateBatch completes a batch and publishes everything that is next
// in the collection's commit order. A nil publish means there is nothing to
// deliver: the batch gives its claim or its queue place back at once. A batch
// with something to deliver that committed nothing is ordered at the point it
// finishes.
//
// Callers must not hold appStateSyncLock or a collection lock: batches of
// other callers may be published from here, and handlers are free to fetch.
func (cli *Client) finishAppStateBatch(batch *appStateBatch, publish func()) {
	if batch == nil {
		return
	}
	p := &cli.appStatePublisher
	name := batch.name
	p.mu.Lock()
	defer p.mu.Unlock()
	if batch.ready {
		return
	}
	batch.ready, batch.publish = true, publish
	switch {
	case publish != nil:
		p.enqueueLocked(batch)
	case batch.claimed:
		p.unclaimLocked(batch)
	case batch.queued:
		p.pending[name] = slices.DeleteFunc(p.pending[name], func(queued *appStateBatch) bool { return queued == batch })
	default:
		return
	}
	if !p.draining[name] && p.headReadyLocked(name) {
		p.draining[name] = true
		for p.headReadyLocked(name) {
			next := p.pending[name][0]
			p.pending[name][0] = nil
			p.pending[name] = p.pending[name][1:]
			if next.publish == nil {
				continue
			}
			p.publishing[name] = next.since
			p.mu.Unlock()
			cli.runAppStatePublish(next)
			p.mu.Lock()
			delete(p.publishing, name)
			cli.appStateBacklogChangedLocked(name)
		}
		delete(p.draining, name)
	}
	if len(p.pending[name]) == 0 {
		delete(p.pending, name)
	}
	cli.appStateBacklogChangedLocked(name)
}

// appStateBacklogChangedLocked wakes the waiters and, as capacity may have
// come back, resumes a catch-up that was waiting for it.
func (cli *Client) appStateBacklogChangedLocked(name appstate.WAPatchName) {
	p := &cli.appStatePublisher
	if p.changed != nil {
		close(p.changed)
		p.changed = nil
	}
	if len(p.owed)+len(p.recoveries) > 0 {
		cli.resumeAppStateCatchUpLocked(name)
	}
}

func (cli *Client) runAppStatePublish(batch *appStateBatch) {
	defer func() {
		if err := recover(); err != nil {
			cli.Log.Errorf("Publishing app state %s batch panicked: %v\n%s", batch.name, err, debug.Stack())
		}
	}()
	batch.publish()
}

func (cli *Client) dispatchAppStateBatch(batch *appStateBatch, eventsToDispatch []any) {
	if len(eventsToDispatch) == 0 {
		cli.finishAppStateBatch(batch, nil)
		return
	}
	cli.finishAppStateBatch(batch, func() {
		for _, evt := range eventsToDispatch {
			cli.dispatchEvent(evt)
		}
	})
}
