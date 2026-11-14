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
	// 20 and 40 seconds apart, and is then put on hold.
	appStateCatchUpMaxFailures = 5
)

// appStateDebt is the fetch owed to a collection after a refusal: the server
// has, or may have, changes that were not applied, and whoever was refused
// is not going to ask again (a server notification is consumed).
//
// The client pays it with an ordinary incremental fetch, one at a time per
// collection, started outside every lock. It is settled by any fetch of the
// collection whose last round with the server began after the last refusal.
// Versions are not compared: after a server reset to a lower cursor the
// notified version is never reached.
//
// What a debt waits for is its state. appStateDebtNext is the whole machine,
// appStateDebtEventLocked the only code that moves a debt through it, and
// appStateDebtStarts the only rule that starts an attempt.
type appStateDebt struct {
	version  uint64 // highest version a notification announced; diagnostics only
	stamp    uint64 // publisher.refusals at the last refusal
	state    appStateDebtState
	failures int         // consecutive failed attempts
	timer    *time.Timer // armed exactly while the state is debtBackoff
	retry    uint64      // which timer that is: a cancelled one may still fire
	offline  bool        // set exactly while the state is debtOffline or debtOnHold
}

func (debt *appStateDebt) stopTimer() {
	if debt.timer != nil {
		debt.timer.Stop()
		debt.timer = nil
	}
}

// appStateDebtState is what a debt waits for while no attempt is running.
// Every wait is for a named event that the client signals itself.
type appStateDebtState uint8

const (
	// debtReady waits for nothing of its own. Its attempt is running, or
	// starts with the next kick: when the worker of its collection ends,
	// when a batch is delivered or gives its capacity back, and on
	// ResumeAppState (appStateBacklogChangedLocked, ResumeAppState).
	debtReady appStateDebtState = iota
	// debtBackoff waits for its retry timer (retryAppStateCatchUp).
	debtBackoff
	// debtOffline waits for the next connection (resumeAppStateCatchUps).
	debtOffline
	// debtOnHold gave up after appStateCatchUpMaxFailures attempts. It waits
	// for the next connection, or for a new refusal: a new reason to try.
	debtOnHold
)

type appStateDebtEvent uint8

const (
	debtRefused             appStateDebtEvent = iota // new work for the collection was refused
	debtConnected                                    // the client logged in
	debtDisconnected                                 // the socket went away, or Disconnect was called
	debtPaused                                       // PauseAppState
	debtTimerFired                                   // the retry timer of the current backoff fired
	debtAttemptNoConnection                          // an attempt ended without a connection
	debtAttemptFailed                                // an attempt ended with any other failure
	debtEvents
)

type appStateDebtEffect uint8

const (
	debtNoEffect       appStateDebtEffect = iota
	debtAnnounceParked                    // tell the application that nothing is scheduled
	debtAnnounceGaveUp                    // the same, after the last failed attempt
	debtArmRetry                          // arm the retry timer and announce the retry
)

// appStateDebtNext is the state machine of an owed fetch.
//
//	state    | refused | connected | disconnected | paused  | timer fired | attempt: no connection | attempt: failed
//	ready    | ready   | ready     | offline *    | ready   | ready       | offline *              | backoff + (paused: ready; 5th: on hold *)
//	backoff  | backoff | backoff   | offline *    | ready   | ready       | backoff                | backoff
//	offline  | offline | ready 0   | offline      | offline | offline     | offline                | offline
//	on hold  | ready 0 | ready 0   | offline      | on hold | on hold     | on hold                | on hold
//
// "0" resets the failure count. "*" announces that nothing is scheduled
// (events.AppStateCatchUpFailed without a RetryIn), "+" arms the retry timer
// and announces the retry. A failure counts only in ready, where it belongs
// to an attempt nothing else has overtaken: after a disconnect it changes
// nothing, and above all arms no timer. While app state is paused no timer
// is armed either; the failure is counted and the resume starts the next
// attempt.
//
// Other things happen to a debt without moving it: capacity coming back, the
// resume, and an attempt that ended refused again or without covering the
// last refusal only kick it (appStateDebtStarts). Being settled removes it,
// from any state.
func appStateDebtNext(state appStateDebtState, event appStateDebtEvent, paused bool, failures int) (appStateDebtState, int, appStateDebtEffect) {
	switch state {
	case debtReady:
		switch event {
		case debtDisconnected, debtAttemptNoConnection:
			return debtOffline, failures, debtAnnounceParked
		case debtAttemptFailed:
			failures++
			if failures >= appStateCatchUpMaxFailures {
				return debtOnHold, failures, debtAnnounceGaveUp
			} else if paused {
				return debtReady, failures, debtNoEffect
			}
			return debtBackoff, failures, debtArmRetry
		}
	case debtBackoff:
		switch event {
		case debtDisconnected:
			return debtOffline, failures, debtAnnounceParked
		case debtPaused, debtTimerFired:
			return debtReady, failures, debtNoEffect
		}
	case debtOffline:
		if event == debtConnected {
			return debtReady, 0, debtNoEffect
		}
	case debtOnHold:
		switch event {
		case debtRefused, debtConnected:
			return debtReady, 0, debtNoEffect
		case debtDisconnected:
			return debtOffline, failures, debtNoEffect
		}
	}
	return state, failures, debtNoEffect
}

// appStateDebtStarts is the only rule that starts an attempt: the debt waits
// for nothing, app state is not paused, no worker (an attempt, or the
// adoption of a deferred recovery snapshot) is running for the collection,
// and the backlog has capacity. It is evaluated after every event above and
// at every kick, so a debt is never left ready with nothing to wake it.
func appStateDebtStarts(state appStateDebtState, paused, busy, capacity bool) bool {
	return state == debtReady && !paused && !busy && capacity
}

// appStateDebtEventLocked moves a debt through appStateDebtNext, applies the
// effect, and starts the attempt if it may start now. cause is the error
// that the announcement of an effect carries. It returns the effect and the
// delay of a retry it armed, for the log.
func (cli *Client) appStateDebtEventLocked(name appstate.WAPatchName, debt *appStateDebt, event appStateDebtEvent, cause error) (appStateDebtEffect, time.Duration) {
	p := &cli.appStatePublisher
	next, failures, effect := appStateDebtNext(debt.state, event, p.paused, debt.failures)
	if next != debtBackoff || effect == debtArmRetry {
		debt.stopTimer()
	}
	debt.state, debt.failures = next, failures
	debt.offline = next == debtOffline || next == debtOnHold
	var retryIn time.Duration
	switch effect {
	case debtAnnounceParked:
		cli.noticeAppStateLocked(&events.AppStateCatchUpFailed{Name: name, Version: debt.version, Error: cause})
	case debtAnnounceGaveUp:
		cli.noticeAppStateLocked(&events.AppStateCatchUpFailed{
			Name: name, Version: debt.version, Error: fmt.Errorf("gave up after %d attempts: %w", failures, cause),
		})
	case debtArmRetry:
		retryIn = p.retryBase
		if retryIn <= 0 {
			retryIn = appStateCatchUpBackoff
		}
		retryIn <<= failures - 1
		debt.retry++
		retry := debt.retry
		debt.timer = time.AfterFunc(retryIn, func() { cli.retryAppStateCatchUp(name, debt, retry) })
		cli.noticeAppStateLocked(&events.AppStateCatchUpFailed{Name: name, Version: debt.version, Error: cause, RetryIn: retryIn})
	}
	cli.resumeAppStateCatchUpLocked(name)
	return effect, retryIn
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
			// Refused while the socket is known to be down, it waits for
			// the connection like every other debt.
			debt = &appStateDebt{}
			if p.disconnected {
				debt.state, debt.offline = debtOffline, true
			}
			p.owed[batch.name] = debt
		}
		p.refusals++
		debt.stamp = p.refusals
		debt.version = max(debt.version, batch.version)
		version = debt.version
		// A new refusal is a new reason to try: it renews a debt that gave
		// up, also during a pause, where the attempt starts with the resume.
		cli.appStateDebtEventLocked(batch.name, debt, debtRefused, nil)
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

// resumeAppStateCatchUpLocked is the kick: it starts the worker a collection
// may start now. A deferred recovery snapshot is adopted first; the owed
// fetch waits for that worker to end like for any other (appStateDebtStarts).
func (cli *Client) resumeAppStateCatchUpLocked(name appstate.WAPatchName) {
	p := &cli.appStatePublisher
	recovery, debt := p.recoveries[name], p.owed[name]
	if recovery == nil && debt == nil {
		return
	}
	limit := cli.appStateBacklogLimit()
	capacity := limit <= 0 || p.backlogLocked(name).Depth < limit
	busy := p.catchingUp[name]
	switch {
	case recovery != nil && !p.paused && !busy && capacity:
		p.catchingUp[name] = true
		go cli.adoptDeferredAppStateRecovery(name, recovery)
	case recovery == nil && appStateDebtStarts(debt.state, p.paused, busy, capacity):
		p.catchingUp[name] = true
		attempt := p.catchUp
		if attempt == nil {
			attempt = cli.catchUpAppState
		}
		go attempt(name, p.connects)
	}
}

// resumeAppStateCatchUps is called after a successful (re)connect.
func (cli *Client) resumeAppStateCatchUps() {
	p := &cli.appStatePublisher
	p.mu.Lock()
	defer p.mu.Unlock()
	p.connects++
	p.disconnected = false
	for name, debt := range p.owed {
		cli.appStateDebtEventLocked(name, debt, debtConnected, nil)
	}
}

// parkAppStateDebts is called when the socket goes away, whoever closed it.
// Nothing that is owed may arm a timer or touch a store while the client is
// disconnected: the application may be closing its stores. Every debt waits
// for the next connection.
func (cli *Client) parkAppStateDebts() {
	p := &cli.appStatePublisher
	p.mu.Lock()
	defer p.mu.Unlock()
	cli.parkAppStateDebtsLocked()
}

func (cli *Client) parkAppStateDebtsLocked() {
	p := &cli.appStatePublisher
	p.disconnected = true
	for name, debt := range p.owed {
		cli.appStateDebtEventLocked(name, debt, debtDisconnected, ErrNotConnected)
	}
}

// parkAppStateCatchUps is called by Disconnect. Besides parking the debts
// it drops the deferred recovery snapshots: they are not adopted into stores
// the application may be closing.
func (cli *Client) parkAppStateCatchUps() {
	p := &cli.appStatePublisher
	p.mu.Lock()
	defer p.mu.Unlock()
	cli.parkAppStateDebtsLocked()
	for name, recovery := range p.recoveries {
		cli.dropAppStateRecoveryLocked(name, recovery, ErrNotConnected)
	}
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
	effect, retryIn := debtNoEffect, time.Duration(0)
	p.mu.Lock()
	delete(p.catchingUp, name)
	if debt := p.owed[name]; debt != nil && err != nil {
		switch {
		case errors.Is(err, ErrAppStateBacklog), errors.Is(err, ErrAppStatePaused):
			// Refused again: the kick below, or a later one, starts over.
		case errors.Is(err, ErrNotConnected), errors.Is(err, ErrIQDisconnected), errors.Is(err, context.Canceled):
			// Wait for the next connection, unless it came during the attempt.
			if p.connects == connects {
				effect, retryIn = cli.appStateDebtEventLocked(name, debt, debtAttemptNoConnection, err)
			}
		default:
			effect, retryIn = cli.appStateDebtEventLocked(name, debt, debtAttemptFailed, err)
		}
	}
	cli.appStateBacklogChangedLocked(name)
	p.mu.Unlock()
	switch effect {
	case debtAnnounceGaveUp:
		cli.Log.Errorf("Giving up on catching up app state %s after its event backlog until the next connection or refusal: %v", name, err)
	case debtArmRetry:
		cli.Log.Errorf("Failed to catch up app state %s after its event backlog, retrying in %s: %v", name, retryIn, err)
	}
}

func (cli *Client) retryAppStateCatchUp(name appstate.WAPatchName, debt *appStateDebt, retry uint64) {
	p := &cli.appStatePublisher
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.owed[name] == debt && debt.timer != nil && debt.retry == retry {
		cli.appStateDebtEventLocked(name, debt, debtTimerFired, nil)
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
