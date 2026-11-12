// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"context"
	"runtime/debug"
	"sync"

	"go.mau.fi/whatsmeow/appstate"
)

// appStateBatch is the event batch of one fetch, conflict response or
// recovery. It takes its place in the collection's publication queue when its
// first write commits, while the collection lock is still held, so the queue
// order is the commit order regardless of which goroutine publishes first.
type appStateBatch struct {
	name    appstate.WAPatchName
	queued  bool
	ready   bool
	publish func()
}

type appStateBatchKey struct{}

// appStatePublisher publishes the batches of each collection in commit order.
// Nobody waits for another batch: a goroutine that finishes a batch while an
// earlier one is still being published leaves it queued, and the goroutine
// publishing that earlier batch delivers it next. A handler may therefore
// fetch the collection it is being called for without deadlocking, and no
// lock is held while handlers run.
type appStatePublisher struct {
	mu       sync.Mutex
	pending  map[appstate.WAPatchName][]*appStateBatch
	draining map[appstate.WAPatchName]bool
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
	}
	batch.queued = true
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
		p.mu.Unlock()
		cli.runAppStatePublish(next)
		p.mu.Lock()
	}
	if len(p.pending[batch.name]) == 0 {
		delete(p.pending, batch.name)
	}
	delete(p.draining, batch.name)
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
