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
	"slices"
	"time"

	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/proto/waSyncdSnapshotRecovery"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

const (
	appStateCatchUpBackoff = 5 * time.Second
	// A fetch that keeps failing is tried this many times in a row, 5, 10,
	// 20 and 40 seconds apart, and then waits for the next connection.
	appStateCatchUpMaxFailures = 5
)

// appStateDebt is the fetch owed to a collection after a refusal: the server
// has, or may have, changes that were not applied, and whoever was refused
// is not going to ask again (a server notification is consumed).
//
// The client pays it with an ordinary incremental fetch, one at a time per
// collection, started outside every lock and only while it is connected and
// not paused: when the backlog has capacity again, after a (re)connect, and
// after a failed attempt once its backoff has passed. It is settled by any
// fetch of the collection whose last round with the server began after the
// last refusal. Versions are not compared: after a server reset to a lower
// cursor the notified version is never reached.
type appStateDebt struct {
	version  uint64      // highest version a notification announced; diagnostics only
	stamp    uint64      // publisher.refusals at the last refusal
	failures int         // consecutive failed attempts
	timer    *time.Timer // set while it waits out a backoff
	retry    uint64      // which timer that is: a cancelled one may still fire
	offline  bool        // set while it waits for the next connection
}

func (debt *appStateDebt) stopTimer() {
	if debt.timer != nil {
		debt.timer.Stop()
		debt.timer = nil
	}
}

// deferredAppStateRecovery is a recovery snapshot that the backlog refused.
// Nothing fetched later can stand in for it, so the latest one per collection
// is kept and adopted, through the same path as on arrival, when the backlog
// has capacity again. It is dropped when the client is paused or
// disconnected: an application that still needs it asks the primary again.
type deferredAppStateRecovery struct {
	reqID    types.MessageID
	snapshot *waSyncdSnapshotRecovery.SyncdSnapshotRecovery
	version  uint64
}

// deferAppStateLocked remembers what a refused batch skipped, and queues the
// notice for the application unless the refusal is a pause. It is called in
// the critical section that refused the claim, so capacity cannot come back
// unnoticed in between. It returns the version for the diagnostics.
func (cli *Client) deferAppStateLocked(batch *appStateBatch, backlog AppStateBacklog) uint64 {
	p := &cli.appStatePublisher
	p.initLocked()
	if batch.quiet {
		// A catch-up attempt: what it is for is already remembered, or was
		// settled or dropped meanwhile and must not come back.
		return batch.version
	}
	version := batch.version
	if recovery := batch.recovery; recovery != nil {
		if p.paused {
			p.dropped[batch.name] = true
			return version
		}
		if kept := p.recoveries[batch.name]; kept == nil || kept.version <= recovery.version {
			p.recoveries[batch.name] = recovery
		}
	} else {
		debt := p.owed[batch.name]
		if debt == nil {
			debt = &appStateDebt{}
			p.owed[batch.name] = debt
		}
		p.refusals++
		debt.stamp = p.refusals
		debt.version = max(debt.version, batch.version)
		version = debt.version
		if !p.paused {
			// New changes are a new reason to try: a debt that gave up, or
			// that waits for a connection that is there by now, starts over.
			// An attempt that finds no connection parks it again.
			debt.failures, debt.offline = 0, false
		}
	}
	if !p.paused {
		cli.noticeAppStateLocked(&events.AppStateDeferred{
			Name: batch.name, Source: batch.source, Version: version,
			Depth: backlog.Depth, OldestAge: backlog.OldestAge,
		})
	}
	return version
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
	debt.stopTimer()
	delete(p.owed, name)
	cli.noticeAppStateLocked(&events.AppStateCaughtUp{Name: name, Version: debt.version})
	p.mu.Unlock()
	cli.Log.Infof("App state %s caught up after its event backlog (notified version %d)", name, debt.version)
}

// resumeAppStateCatchUpLocked starts the attempt for what is not waiting for
// anything: a deferred recovery snapshot first, then an owed fetch. It is
// called after everything that can end a wait.
func (cli *Client) resumeAppStateCatchUpLocked(name appstate.WAPatchName) {
	p := &cli.appStatePublisher
	if p.paused || p.catchingUp[name] {
		return
	}
	recovery, debt := p.recoveries[name], p.owed[name]
	if recovery == nil && (debt == nil || debt.timer != nil || debt.offline) {
		return
	}
	if limit := cli.appStateBacklogLimit(); limit > 0 && p.backlogLocked(name).Depth >= limit {
		return
	}
	p.catchingUp[name] = true
	if recovery != nil {
		go cli.adoptDeferredAppStateRecovery(name, recovery)
	} else {
		go cli.catchUpAppState(name, p.connects)
	}
}

// resumeAppStateCatchUps is called after a successful (re)connect.
func (cli *Client) resumeAppStateCatchUps() {
	p := &cli.appStatePublisher
	p.mu.Lock()
	defer p.mu.Unlock()
	p.connects++
	for name, debt := range p.owed {
		debt.failures, debt.offline = 0, false
		cli.resumeAppStateCatchUpLocked(name)
	}
}

// parkAppStateCatchUps is called by Disconnect. Nothing that is owed may
// touch a store or schedule work once the application has disconnected: it
// may be closing its stores. Every debt waits for the next connection, and
// deferred recovery snapshots are dropped.
func (cli *Client) parkAppStateCatchUps() {
	p := &cli.appStatePublisher
	p.mu.Lock()
	defer p.mu.Unlock()
	for name, debt := range p.owed {
		cli.parkAppStateDebtLocked(name, debt, ErrNotConnected)
	}
	for name, recovery := range p.recoveries {
		cli.dropAppStateRecoveryLocked(name, recovery, ErrNotConnected)
	}
}

// parkAppStateDebtLocked makes a debt wait for the next connection, and says
// so once: a retry that was announced is not going to happen.
func (cli *Client) parkAppStateDebtLocked(name appstate.WAPatchName, debt *appStateDebt, reason error) {
	debt.stopTimer()
	if debt.offline {
		return
	}
	debt.offline = true
	cli.noticeAppStateLocked(&events.AppStateCatchUpFailed{Name: name, Version: debt.version, Error: reason})
}

func (cli *Client) dropAppStateRecoveryLocked(name appstate.WAPatchName, recovery *deferredAppStateRecovery, reason error) {
	p := &cli.appStatePublisher
	if p.recoveries[name] != recovery {
		return
	}
	delete(p.recoveries, name)
	p.dropped[name] = true
	cli.noticeAppStateLocked(&events.AppStateCatchUpFailed{
		Name: name, Version: recovery.version,
		Error: fmt.Errorf("deferred recovery snapshot v%d was dropped: %w", recovery.version, reason),
	})
}

func (cli *Client) catchUpAppState(name appstate.WAPatchName, connects uint64) {
	ctx := cli.BackgroundEventCtx
	if ctx == nil {
		ctx = context.Background()
	}
	var err error
	defer func() { cli.endAppStateCatchUp(name, connects, err) }()
	// Before anything that reads a store: see parkAppStateCatchUps.
	if !cli.IsConnected() {
		err = ErrNotConnected
		return
	}
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
	gaveUp := false
	p.mu.Lock()
	delete(p.catchingUp, name)
	if debt := p.owed[name]; debt != nil && err != nil {
		switch {
		case errors.Is(err, ErrAppStateBacklog), errors.Is(err, ErrAppStatePaused):
			// Refused again. No timer: the next delivered batch, or the
			// resume, starts the next attempt.
		case errors.Is(err, ErrNotConnected), errors.Is(err, ErrIQDisconnected), errors.Is(err, context.Canceled):
			// Wait for the next connection, unless it came during the attempt.
			if p.connects == connects {
				cli.parkAppStateDebtLocked(name, debt, err)
			}
		default:
			debt.failures++
			if gaveUp = debt.failures >= appStateCatchUpMaxFailures; gaveUp {
				cli.parkAppStateDebtLocked(name, debt, fmt.Errorf("gave up after %d attempts: %w", debt.failures, err))
				break
			}
			retryIn = p.retryBase
			if retryIn <= 0 {
				retryIn = appStateCatchUpBackoff
			}
			retryIn <<= debt.failures - 1
			debt.retry++
			retry := debt.retry
			debt.timer = time.AfterFunc(retryIn, func() { cli.retryAppStateCatchUp(name, debt, retry) })
			cli.noticeAppStateLocked(&events.AppStateCatchUpFailed{Name: name, Version: debt.version, Error: err, RetryIn: retryIn})
		}
	}
	cli.appStateBacklogChangedLocked(name)
	p.mu.Unlock()
	if gaveUp {
		cli.Log.Errorf("Giving up on catching up app state %s after its event backlog until the next connection or fetch: %v", name, err)
	} else if retryIn > 0 {
		cli.Log.Errorf("Failed to catch up app state %s after its event backlog, retrying in %s: %v", name, retryIn, err)
	}
}

func (cli *Client) retryAppStateCatchUp(name appstate.WAPatchName, debt *appStateDebt, retry uint64) {
	p := &cli.appStatePublisher
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.owed[name] == debt && debt.timer != nil && debt.retry == retry {
		debt.timer = nil
		cli.resumeAppStateCatchUpLocked(name)
	}
}

// adoptDeferredAppStateRecovery adopts a snapshot that was refused on
// arrival. Whatever comes of it, it is not tried twice: refused again it
// stays for the next drain, adopted it is announced as caught up, and
// anything else is reported and dropped, as it would have been on arrival.
func (cli *Client) adoptDeferredAppStateRecovery(name appstate.WAPatchName, recovery *deferredAppStateRecovery) {
	ctx := cli.BackgroundEventCtx
	if ctx == nil {
		ctx = context.Background()
	}
	err := ErrNotConnected
	// Before anything that reads a store: see parkAppStateCatchUps.
	if cli.IsConnected() {
		_, err = cli.adoptAppStateRecovery(ctx, recovery, true)
	}
	p := &cli.appStatePublisher
	p.mu.Lock()
	delete(p.catchingUp, name)
	refused := errors.Is(err, ErrAppStateBacklog) || errors.Is(err, ErrAppStatePaused)
	if err == nil {
		// A newer snapshot may have been deferred meanwhile: it stays.
		if p.recoveries[name] == recovery {
			delete(p.recoveries, name)
		}
		cli.noticeAppStateLocked(&events.AppStateCaughtUp{Name: name, Version: recovery.version})
	} else if !refused {
		cli.dropAppStateRecoveryLocked(name, recovery, err)
	}
	cli.appStateBacklogChangedLocked(name)
	p.mu.Unlock()
	if err != nil && !refused {
		cli.Log.Errorf("Dropped the app state %s recovery snapshot v%d that was deferred by the event backlog (%s): %v", name, recovery.version, recovery.reqID, err)
	}
}

func appStateNoticeKey(evt any) (kind int, name appstate.WAPatchName) {
	switch v := evt.(type) {
	case *events.AppStateDeferred:
		return 1, v.Name
	case *events.AppStateCaughtUp:
		return 2, v.Name
	case *events.AppStateCatchUpFailed:
		return 3, v.Name
	}
	return 0, ""
}

// noticeAppStateLocked queues an event about the backlog itself. These are
// not app state batches: they go to the event handlers in order, on their
// own goroutine, also while a handler holds a batch of the collection.
//
// While a handler keeps them from being delivered, only the latest notice of
// each kind per collection is kept, at the end of the queue: there are at
// most three per collection, and the last one delivered for a collection is
// its latest state. Every refusal is logged as well.
func (cli *Client) noticeAppStateLocked(evt any) {
	p := &cli.appStatePublisher
	kind, name := appStateNoticeKey(evt)
	p.notices = slices.DeleteFunc(p.notices, func(queued any) bool {
		queuedKind, queuedName := appStateNoticeKey(queued)
		return queuedKind == kind && queuedName == name
	})
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
		p.notices = slices.Delete(p.notices, 0, 1)
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
