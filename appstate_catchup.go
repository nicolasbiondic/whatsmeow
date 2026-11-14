// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"context"
	"errors"
	"time"

	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/types/events"
)

const (
	appStateCatchUpBackoff    = 5 * time.Second
	appStateCatchUpMaxBackoff = 5 * time.Minute
	// Every notice is also logged, so the ones over this are only logged.
	maxAppStateNotices = 64
)

// appStateDebt is the fetch owed to a collection after a backlog refusal:
// the server has, or may have, changes that were not applied, and whoever
// was refused is not going to ask again (a server notification is consumed).
//
// The client pays it with an ordinary incremental fetch, one at a time per
// collection, started outside every lock: when the backlog has capacity
// again, after a (re)connect, and after a failed attempt once its backoff
// has passed. It is settled by any fetch of the collection whose last round
// with the server began after the last refusal. Versions are not compared:
// after a server reset to a lower cursor the notified version is never
// reached.
type appStateDebt struct {
	version  uint64      // highest version a notification announced; diagnostics only
	stamp    uint64      // publisher.refusals at the last refusal
	failures int         // consecutive failed attempts, for the backoff
	timer    *time.Timer // set while it waits out a backoff
	offline  bool        // set while it waits for a connection
}

// oweAppStateFetchLocked records the debt of a refused batch and queues the
// notice for the application. It is called in the critical section that
// refused the claim, so capacity cannot come back unnoticed in between.
func (cli *Client) oweAppStateFetchLocked(batch *appStateBatch, backlog AppStateBacklog) uint64 {
	p := &cli.appStatePublisher
	p.initLocked()
	debt := p.owed[batch.name]
	if batch.quiet {
		// A catch-up attempt adds nothing to a debt, and does not revive one
		// that an ordinary fetch settled meanwhile.
		if debt == nil {
			return 0
		}
		return debt.version
	}
	if debt == nil {
		debt = &appStateDebt{}
		p.owed[batch.name] = debt
	}
	p.refusals++
	debt.stamp = p.refusals
	debt.version = max(debt.version, batch.version)
	cli.noticeAppStateLocked(&events.AppStateDeferred{
		Name: batch.name, Source: batch.source, Version: debt.version,
		Depth: backlog.Depth, OldestAge: backlog.OldestAge,
	})
	return debt.version
}

// appStateCatchUpMark is taken by a fetch right before it asks the server:
// its answer covers every refusal up to here, and none after.
func (cli *Client) appStateCatchUpMark() uint64 {
	p := &cli.appStatePublisher
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.refusals
}

// settleAppStateCatchUp is called by a fetch that completed with the server.
func (cli *Client) settleAppStateCatchUp(name appstate.WAPatchName, mark uint64) {
	p := &cli.appStatePublisher
	p.mu.Lock()
	debt := p.owed[name]
	if debt == nil || debt.stamp > mark {
		p.mu.Unlock()
		return
	}
	if debt.timer != nil {
		debt.timer.Stop()
	}
	delete(p.owed, name)
	cli.noticeAppStateLocked(&events.AppStateCaughtUp{Name: name, Version: debt.version})
	p.mu.Unlock()
	cli.Log.Infof("App state %s caught up after its event backlog (notified version %d)", name, debt.version)
}

// resumeAppStateCatchUpLocked starts the attempt for a debt that is not
// waiting for anything. It is called after everything that can end a wait.
func (cli *Client) resumeAppStateCatchUpLocked(name appstate.WAPatchName) {
	p := &cli.appStatePublisher
	debt := p.owed[name]
	if debt == nil || debt.timer != nil || debt.offline || p.catchingUp[name] {
		return
	}
	if limit := cli.appStateBacklogLimit(); limit > 0 && p.backlogLocked(name).Depth >= limit {
		return
	}
	p.catchingUp[name] = true
	go cli.catchUpAppState(name, p.connects)
}

// resumeAppStateCatchUps is called after a successful (re)connect.
func (cli *Client) resumeAppStateCatchUps() {
	p := &cli.appStatePublisher
	p.mu.Lock()
	defer p.mu.Unlock()
	p.connects++
	for name, debt := range p.owed {
		debt.offline = false
		cli.resumeAppStateCatchUpLocked(name)
	}
}

func (cli *Client) catchUpAppState(name appstate.WAPatchName, connects uint64) {
	ctx := cli.BackgroundEventCtx
	if ctx == nil {
		ctx = context.Background()
	}
	var err error
	defer func() { cli.endAppStateCatchUp(name, connects, err) }()
	var batch *appStateBatch
	var eventsToDispatch []any
	batch, eventsToDispatch, err = cli.fetchAppStateBatch(ctx, name, false, false, appStateOrigin{source: "catch-up", first: true, quiet: true})
	if err == nil {
		cli.dispatchAppStateBatch(batch, eventsToDispatch)
	}
}

func (cli *Client) endAppStateCatchUp(name appstate.WAPatchName, connects uint64, err error) {
	p := &cli.appStatePublisher
	var retryIn time.Duration
	p.mu.Lock()
	delete(p.catchingUp, name)
	if debt := p.owed[name]; debt != nil && err != nil {
		switch {
		case errors.Is(err, ErrAppStateBacklog):
			// Refused again. No timer: the next delivered batch resumes it.
		case errors.Is(err, ErrNotConnected), errors.Is(err, ErrIQDisconnected), errors.Is(err, context.Canceled):
			// Wait for the next connection, unless it came during the attempt.
			debt.offline = p.connects == connects
		default:
			debt.failures++
			retryIn = p.retryBase
			if retryIn <= 0 {
				retryIn = appStateCatchUpBackoff
			}
			retryIn = min(retryIn<<min(debt.failures-1, 16), appStateCatchUpMaxBackoff)
			debt.timer = time.AfterFunc(retryIn, func() { cli.retryAppStateCatchUp(name, debt) })
			cli.noticeAppStateLocked(&events.AppStateCatchUpFailed{Name: name, Version: debt.version, Error: err, RetryIn: retryIn})
		}
	}
	cli.appStateBacklogChangedLocked(name)
	p.mu.Unlock()
	if retryIn > 0 {
		cli.Log.Errorf("Failed to catch up app state %s after its event backlog, retrying in %s: %v", name, retryIn, err)
	}
}

func (cli *Client) retryAppStateCatchUp(name appstate.WAPatchName, debt *appStateDebt) {
	p := &cli.appStatePublisher
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.owed[name] == debt {
		debt.timer = nil
		cli.resumeAppStateCatchUpLocked(name)
	}
}

// noticeAppStateLocked queues an event about the backlog itself. These are
// not app state batches: they go to the event handlers in order, on their
// own goroutine, also while a handler holds a batch of the collection.
func (cli *Client) noticeAppStateLocked(evt any) {
	p := &cli.appStatePublisher
	if len(p.notices) >= maxAppStateNotices {
		return
	}
	p.notices = append(p.notices, evt)
	if !p.noticing {
		p.noticing = true
		go cli.deliverAppStateNotices()
	}
}

func (cli *Client) deliverAppStateNotices() {
	p := &cli.appStatePublisher
	p.mu.Lock()
	defer p.mu.Unlock()
	for len(p.notices) > 0 {
		evt := p.notices[0]
		p.notices[0] = nil
		p.notices = p.notices[1:]
		p.mu.Unlock()
		cli.dispatchEvent(evt)
		p.mu.Lock()
	}
	p.noticing = false
	if p.changed != nil {
		close(p.changed)
		p.changed = nil
	}
}
