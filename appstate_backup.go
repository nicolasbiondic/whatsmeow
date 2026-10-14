// Non-destructive full sync support: capture the verified local app state
// (version, LTHash and latest mutation MACs) before a full sync deletes it, so
// that a failed sync can restore it instead of leaving the collection empty.
package whatsmeow

import (
	"context"
	"fmt"

	"go.mau.fi/whatsmeow/store"
)

// appStateVersionBackup holds everything needed to put an app state collection
// back exactly as it was before a full sync deleted it.
type appStateVersionBackup struct {
	Version uint64
	Hash    [128]byte
	MACs    []store.AppStateMutationMAC
}

// appStateMutationMACDumper is implemented by stores (like this fork's
// sqlstore) that can enumerate the latest value MAC for every stored index MAC
// of a collection.
type appStateMutationMACDumper interface {
	GetAllAppStateMutationMACs(ctx context.Context, name string) ([]store.AppStateMutationMAC, error)
}

// captureAppStateVersion snapshots the current version, hash and latest
// mutation MACs of the given collection. It returns nil (and no error) when
// there is nothing stored (version 0), and an error when the store cannot
// enumerate mutation MACs or any read fails.
func captureAppStateVersion(ctx context.Context, s store.AppStateStore, name string) (*appStateVersionBackup, error) {
	dumper, ok := s.(appStateMutationMACDumper)
	if !ok {
		return nil, fmt.Errorf("app state store %T does not support enumerating mutation MACs", s)
	}
	version, hash, err := s.GetAppStateVersion(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("get app state version: %w", err)
	}
	if version == 0 {
		return nil, nil
	}
	macs, err := dumper.GetAllAppStateMutationMACs(ctx, name)
	if err != nil {
		return nil, fmt.Errorf("get app state mutation MACs: %w", err)
	}
	return &appStateVersionBackup{Version: version, Hash: hash, MACs: macs}, nil
}

// restoreAppStateVersionIfOlder restores the backup unless the collection's
// stored version has meanwhile reached backup.Version or newer — which means a
// recovery response (or a partially verified sync) persisted an equal-or-newer
// state while the failed full sync was running, and putting the captured copy
// back would regress it. Returns whether the backup was written.
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
