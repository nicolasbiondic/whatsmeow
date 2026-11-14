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

// AppStateBacklogError reports that nothing was fetched, sent or stored,
// because the collection's event backlog is at Client.AppStateBacklogLimit.
// It is transient: retry once the event handlers have caught up.
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

// AppStateBacklog describes the committed event batches of one collection
// that the event handlers have not finished yet.
type AppStateBacklog struct {
	// Depth counts the batch being dispatched and those queued behind it.
	Depth int
	// OldestAge is the time since the oldest of them was committed.
	OldestAge time.Duration
}

// appStateBatch is the event batch of one fetch, conflict response or
// recovery. It takes its place in the collection's publication queue when its
// first write commits, while the collection lock is still held, so the queue
// order is the commit order regardless of which goroutine publishes first.
type appStateBatch struct {
	name    appstate.WAPatchName
	queued  bool
	ready   bool
	since   time.Time
	publish func()
}

type appStateBatchKey struct{}

// appStatePublisher publishes the batches of each collection in commit order.
// Nobody waits for another batch: a goroutine that finishes a batch while an
// earlier one is still being published leaves it queued, and the goroutine
// publishing that earlier batch delivers it next. A handler may therefore
// fetch the collection it is being called for without deadlocking, and no
// lock is held while handlers run.
//
// Committed batches are never dropped and producers are never blocked, so the
// queue is bounded at admission instead: see admitAppStateBatch.
type appStatePublisher struct {
	mu       sync.Mutex
	pending  map[appstate.WAPatchName][]*appStateBatch
	draining map[appstate.WAPatchName]bool
	// publishing holds the commit time of the batch that is in the handlers.
	publishing map[appstate.WAPatchName]time.Time
	// drained is closed whenever a collection's queue runs empty.
	drained chan struct{}
}

// AppStateBacklog returns how many committed event batches of the collection
// the event handlers have not finished, and how old the oldest one is.
func (cli *Client) AppStateBacklog(name appstate.WAPatchName) AppStateBacklog {
	p := &cli.appStatePublisher
	p.mu.Lock()
	defer p.mu.Unlock()
	backlog := AppStateBacklog{Depth: len(p.pending[name])}
	oldest, publishing := p.publishing[name]
	if publishing {
		backlog.Depth++
	} else if backlog.Depth > 0 {
		oldest = p.pending[name][0].since
	}
	if backlog.Depth > 0 {
		backlog.OldestAge = time.Since(oldest)
	}
	return backlog
}

// admitAppStateBatch refuses new work for a collection whose backlog is at
// the limit, before anything is fetched or stored. It never waits, so a
// handler that fetches while its own batch is the backlog gets the error
// instead of a deadlock, and what it would have fetched stays on the server.
func (cli *Client) admitAppStateBatch(name appstate.WAPatchName) error {
	limit := cli.AppStateBacklogLimit
	if limit == 0 {
		limit = DefaultAppStateBacklogLimit
	}
	if backlog := cli.AppStateBacklog(name); limit > 0 && backlog.Depth >= limit {
		return &AppStateBacklogError{Name: name, AppStateBacklog: backlog}
	}
	return nil
}

// WaitForAppStateEvents blocks until the event handlers have finished every
// committed app state batch, or until ctx ends.
//
// Disconnect neither waits for these batches nor drops them: their state is
// already stored, so dropping them would lose them for the application, and
// waiting would deadlock a handler that disconnects. They keep being
// delivered on the goroutines that are already dispatching. Call this after
// Disconnect and before removing event handlers or closing what they write
// to. Do not call it from an event handler, which would wait for itself.
func (cli *Client) WaitForAppStateEvents(ctx context.Context) error {
	p := &cli.appStatePublisher
	for {
		p.mu.Lock()
		busy := len(p.pending) + len(p.draining)
		if busy > 0 && p.drained == nil {
			p.drained = make(chan struct{})
		}
		drained := p.drained
		p.mu.Unlock()
		if busy == 0 {
			return nil
		}
		select {
		case <-drained:
		case <-ctx.Done():
			return fmt.Errorf("app state events are still being delivered: %w", ctx.Err())
		}
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

func (p *appStatePublisher) reserve(batch *appStateBatch) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.enqueueLocked(batch)
}

func (p *appStatePublisher) enqueueLocked(batch *appStateBatch) {
	if batch.queued {
		return
	}
	if p.pending == nil {
		p.pending = make(map[appstate.WAPatchName][]*appStateBatch)
		p.draining = make(map[appstate.WAPatchName]bool)
		p.publishing = make(map[appstate.WAPatchName]time.Time)
	}
	batch.queued, batch.since = true, time.Now()
	p.pending[batch.name] = append(p.pending[batch.name], batch)
}

// finishAppStateBatch completes a batch and publishes everything that is next
// in the collection's commit order. A nil publish abandons the batch: nothing
// is delivered, but later batches stop waiting for it. A batch that committed
// nothing is ordered at the point it finishes.
//
// Callers must not hold appStateSyncLock or a collection lock: batches of
// other callers may be published from here, and handlers are free to fetch.
func (cli *Client) finishAppStateBatch(batch *appStateBatch, publish func()) {
	if batch == nil {
		return
	}
	p := &cli.appStatePublisher
	p.mu.Lock()
	if batch.ready {
		p.mu.Unlock()
		return
	}
	batch.ready, batch.publish = true, publish
	if publish != nil {
		p.enqueueLocked(batch)
	}
	if !batch.queued || p.draining[batch.name] {
		p.mu.Unlock()
		return
	}
	p.draining[batch.name] = true
	for {
		queue := p.pending[batch.name]
		if len(queue) == 0 || !queue[0].ready {
			break
		}
		next := queue[0]
		queue[0] = nil
		p.pending[batch.name] = queue[1:]
		if next.publish == nil {
			continue
		}
		p.publishing[batch.name] = next.since
		p.mu.Unlock()
		cli.runAppStatePublish(next)
		p.mu.Lock()
		delete(p.publishing, batch.name)
	}
	if len(p.pending[batch.name]) == 0 {
		delete(p.pending, batch.name)
	}
	delete(p.draining, batch.name)
	if p.drained != nil {
		close(p.drained)
		p.drained = nil
	}
	p.mu.Unlock()
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
	cli.finishAppStateBatch(batch, func() {
		for _, evt := range eventsToDispatch {
			cli.dispatchEvent(evt)
		}
	})
}
