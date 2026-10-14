// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"context"
	"fmt"

	"go.mau.fi/whatsmeow/store"
)

// appStateVersionBackup holds everything needed to put an app state collection
// back exactly as it was before a full sync deleted it: the version, the
// LTHash and the latest value MAC for every index MAC.
type appStateVersionBackup struct {
	Version uint64
	Hash    [128]byte
	MACs    []store.AppStateMutationMAC
}

// captureAppStateVersion snapshots the current version, hash and latest
// mutation MACs of the given collection. It returns nil (and no error) when
// there is nothing stored (version 0).
func captureAppStateVersion(ctx context.Context, s store.AppStateStore, name string) (*appStateVersionBackup, error) {
	version, hash, err := s.GetAppStateVersion(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("get app state version: %w", err)
	}
	if version == 0 {
		return nil, nil
	}
	macs, err := s.GetAllAppStateMutationMACs(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("get app state mutation MACs: %w", err)
	}
	return &appStateVersionBackup{Version: version, Hash: hash, MACs: macs}, nil
}

// restoreAppStateVersionIfOlder restores the backup unless the collection's
// stored version has meanwhile reached backup.Version or newer — which means a
// recovery response persisted an equal-or-newer state while the failed full
// sync was running, and putting the captured copy back would regress it.
// Returns whether the backup was written.
//
// Callers must hold a lock that excludes concurrent recovery writes (the
// client's appStateRecoveryLock) so the check-then-restore is atomic.
func restoreAppStateVersionIfOlder(ctx context.Context, s store.AppStateStore, name string, backup *appStateVersionBackup) (bool, error) {
	if backup == nil {
		return false, nil
	}
	version, _, err := s.GetAppStateVersion(ctx, name)
	if err != nil {
		return false, fmt.Errorf("get app state version before restore: %w", err)
	}
	if version >= backup.Version {
		return false, nil
	}
	return true, restoreAppStateVersion(ctx, s, name, backup)
}

// restoreAppStateVersion deletes whatever partial state a failed full sync
// left behind and writes the captured version, hash and mutation MACs back.
func restoreAppStateVersion(ctx context.Context, s store.AppStateStore, name string, backup *appStateVersionBackup) error {
	if backup == nil {
		return nil
	}
	// Drop any partial state stored by the failed sync (the version row
	// cascades to its mutation MACs).
	if err := s.DeleteAppStateVersion(ctx, name); err != nil {
		return fmt.Errorf("delete partial app state: %w", err)
	}
	if err := s.PutAppStateVersion(ctx, name, backup.Version, backup.Hash); err != nil {
		return fmt.Errorf("restore app state version: %w", err)
	}
	if len(backup.MACs) > 0 {
		if err := s.PutAppStateMutationMACs(ctx, name, backup.Version, backup.MACs); err != nil {
			return fmt.Errorf("restore app state mutation MACs: %w", err)
		}
	}
	return nil
}
