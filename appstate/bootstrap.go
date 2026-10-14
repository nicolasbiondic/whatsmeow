// Bootstrapping an app state collection from an unverifiable snapshot.
//
// On some LID-migrated accounts no device shares the server's collection
// LTHash: the snapshot the server returns (written by the primary) does not
// verify against the plain sum of its records, every later patch is signed
// with its author's own diverged hash, and the primary never answers recovery
// requests. A client with no local state at all can then never adopt any
// state. WhatsApp Web in the same situation would mark the collection
// isInMacMismatchFatal and try a recovery; when the recovery is impossible,
// the account simply has no verifiable state to offer.
//
// decodeSnapshot therefore accepts a snapshot whose MAC does not verify, with
// the plain-sum hash, ONLY when all of the following hold:
//
//   - the context is marked with WithSnapshotBootstrap, which the client only
//     does when a full sync starts with NO prior local state (a capture error
//     is not "no state": it never enables this);
//   - tolerance is enabled for the collection (see companion.go);
//   - every record decrypts and its content and index MACs verify;
//   - the decrypted records contain at least one LID-indexed chat action,
//     proving the collection is LID-migrated (the only situation in which
//     hashes legitimately diverge).
//
// With prior local state the flag is never set and nothing changes.
package appstate

import (
	"context"
	"slices"
	"time"

	"go.mau.fi/whatsmeow/proto/waServerSync"
	"go.mau.fi/whatsmeow/types"
)

type snapshotBootstrapCtxKey struct{}

// WithSnapshotBootstrap marks the context of a full sync that found no prior
// local state for the collection, allowing decodeSnapshot to adopt a snapshot
// whose MAC does not verify (see this file's doc comment for the conditions).
func WithSnapshotBootstrap(ctx context.Context) context.Context {
	return context.WithValue(ctx, snapshotBootstrapCtxKey{}, true)
}

func isSnapshotBootstrap(ctx context.Context) bool {
	flagged, _ := ctx.Value(snapshotBootstrapCtxKey{}).(bool)
	return flagged
}

// hasLIDChatActionMutation reports whether any decoded mutation is a chat
// action of the given collection indexed by a LID JID.
func hasLIDChatActionMutation(name WAPatchName, mutations []Mutation) bool {
	actions := chatActionIndexes[name]
	if len(actions) == 0 {
		return false
	}
	for _, mutation := range mutations {
		if mutation.Operation != waServerSync.SyncdMutation_SET || len(mutation.Index) != 2 {
			continue
		}
		if !slices.Contains(actions, mutation.Index[0]) {
			continue
		}
		jid, err := types.ParseJID(mutation.Index[1])
		if err == nil && jid.Server == types.HiddenUserServer {
			return true
		}
	}
	return false
}

// noteSnapshotBootstrap records the adoption of an unverifiable snapshot and
// marks the collection as LID-migrated (the LID-indexed records just proved
// it), so later primary patches with diverged hashes are tolerated too.
func (proc *Processor) noteSnapshotBootstrap(name WAPatchName, version uint64, records int) {
	proc.lidMigratedCacheLock.Lock()
	proc.lidMigratedCache[name] = true
	proc.lidMigratedCacheLock.Unlock()
	proc.Log.Warnf("Bootstrapped app state %s from snapshot v%d (%d records) whose MAC does not verify; records are MAC-verified and contain LID chat actions", name, version, records)
	proc.appendDiagRecord(map[string]any{
		"ts":         time.Now().UTC().Format(time.RFC3339Nano),
		"type":       "snapshot_bootstrap_tolerated",
		"collection": string(name),
		"version":    version,
		"records":    records,
	})
}
