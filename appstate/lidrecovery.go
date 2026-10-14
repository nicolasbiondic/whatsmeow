// Experimental LTHash recovery for the WhatsApp PN→LID app state migration
// (see whatsmeow issue #858).
//
// When the primary device migrates 1:1 chats from phone number (PN) JIDs to
// LID JIDs, it mutates the server-side syncd LTHash by subtracting the value
// MACs of PN-indexed records without sending corresponding REMOVE mutations
// to companion devices. A companion that applies a later patch containing a
// SET with a LID index therefore computes a hash that still contains the
// orphaned PN entry's contribution, and snapshot MAC verification fails with
// ErrMismatchingLTHash.
//
// This file implements a mapping-free recovery: since the snapshot MAC is
// HMAC(key, lthash ‖ version ‖ name), we can try subtracting each locally
// stored value MAC (the entries that would become orphans) from the computed
// hash until the MAC verifies. A match proves (up to HMAC-SHA256 forgery,
// i.e. with overwhelming probability) that the subtracted entry is exactly
// what the primary removed, without needing the PN↔LID mapping.
//
// The recovery is only ever attempted after normal verification has failed,
// and its result is only accepted if the snapshot MAC verifies afterwards.
// Nothing is ever uploaded or signed based on an unverified hash: this code
// runs purely in the decode path, and on failure the original error is
// returned unchanged.
//
// Environment variables:
//
//	WHATSMEOW_APPSTATE_LID_RECOVERY=1
//	    Apply the recovery to the local state when it verifies.
//	WHATSMEOW_APPSTATE_DIAG_DIR=<dir>
//	    Diagnostic mode: dump every downloaded (still encrypted) snapshot and
//	    patch for monitored collections to <dir>, run the recovery search in
//	    dry-run mode when verification fails and record whether it would have
//	    matched, WITHOUT modifying local state (unless the first variable is
//	    also set) and without writing anything to the server.
package appstate

import (
	"context"
	"crypto/hmac"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"google.golang.org/protobuf/proto"

	"go.mau.fi/whatsmeow/appstate/lthash"
	"go.mau.fi/whatsmeow/proto/waServerSync"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/util/hkdfutil"
)

const lidRecoveryEnvVar = "WHATSMEOW_APPSTATE_LID_RECOVERY"
const diagDirEnvVar = "WHATSMEOW_APPSTATE_DIAG_DIR"

// maxLTHashRecoveryCandidates caps the candidate set to keep the search cheap
// (k=1 is O(N), k=2 is O(N²) HMACs with precomputed HKDF expansions).
const maxLTHashRecoveryCandidates = 25_000

// maxPairSearchCandidates bounds the O(N²) pair search, which runs inline in
// the app state sync path: ~12s worst case at 3k candidates. Larger stores
// only get the O(N) single-orphan search; deeper searches are done offline
// on the diagnostic dumps.
const maxPairSearchCandidates = 3_000

func lidRecoveryApplyEnabled() bool {
	return os.Getenv(lidRecoveryEnvVar) == "1"
}

func appStateDiagDir() string {
	return os.Getenv(diagDirEnvVar)
}

// allAppStateMutationMACsGetter is an optional extension of store.AppStateStore
// used to enumerate recovery candidates. Implemented by sqlstore.
type allAppStateMutationMACsGetter interface {
	GetAllAppStateMutationMACs(ctx context.Context, name string) ([]store.AppStateMutationMAC, error)
}

// expandForLTHash mirrors the per-item expansion done by
// lthash.WAPatchIntegrity (HKDF-SHA256, no salt, info "WhatsApp Patch
// Integrity", 128 bytes).
func expandForLTHash(valueMAC []byte) (out [128]byte) {
	copy(out[:], hkdfutil.SHA256(valueMAC, nil, []byte("WhatsApp Patch Integrity"), 128))
	return
}

func subtractExpandedInPlace(base *[128]byte, vec *[128]byte) {
	for i := 0; i < 128; i += 2 {
		x := binary.LittleEndian.Uint16(base[i : i+2])
		y := binary.LittleEndian.Uint16(vec[i : i+2])
		binary.LittleEndian.PutUint16(base[i:i+2], x-y)
	}
}

// findMissingPreviousValues searches for a combination of up to maxK candidate
// value MACs which, when subtracted from state.Hash, makes the snapshot MAC
// verify against expectedSnapshotMAC. It returns the matching candidates and
// the corrected hash. maxK is clamped to 2 (the search is O(N^k)).
//
// A false positive would require finding an input on which HMAC-SHA256 with an
// unknown key collides with the expected MAC; with N ≤ 25k candidates and
// k ≤ 2 that is < 2^-220 and can be treated as impossible.
func findMissingPreviousValues(
	state HashState,
	name WAPatchName,
	snapshotMACKey []byte,
	expectedSnapshotMAC []byte,
	candidates []store.AppStateMutationMAC,
	maxK int,
) (found []store.AppStateMutationMAC, fixedHash [128]byte, ok bool) {
	if maxK > 2 {
		maxK = 2
	}
	if maxK == 2 && len(candidates) > maxPairSearchCandidates {
		maxK = 1
	}
	if maxK < 1 || len(candidates) == 0 || len(candidates) > maxLTHashRecoveryCandidates {
		return
	}
	verify := func(hash [128]byte) bool {
		trial := HashState{Version: state.Version, Hash: hash}
		return hmac.Equal(trial.generateSnapshotMAC(name, snapshotMACKey), expectedSnapshotMAC)
	}
	// Precompute the HKDF expansion of every candidate once.
	expanded := make([][128]byte, len(candidates))
	for i, cand := range candidates {
		expanded[i] = expandForLTHash(cand.ValueMAC)
	}
	// k = 1
	for i := range candidates {
		trial := state.Hash
		subtractExpandedInPlace(&trial, &expanded[i])
		if verify(trial) {
			return []store.AppStateMutationMAC{candidates[i]}, trial, true
		}
	}
	if maxK < 2 {
		return
	}
	// k = 2 (unordered pairs, including i==j for a double-counted entry)
	for i := range candidates {
		base := state.Hash
		subtractExpandedInPlace(&base, &expanded[i])
		for j := i; j < len(candidates); j++ {
			trial := base
			subtractExpandedInPlace(&trial, &expanded[j])
			if verify(trial) {
				return []store.AppStateMutationMAC{candidates[i], candidates[j]}, trial, true
			}
		}
	}
	return
}

// tryLTHashRecovery is called from validatePatch after snapshot MAC
// verification of a patch failed with ErrMismatchingLTHash. failedState is the
// (non-verifying) state computed by updateHash. missingPrevSETs is the number
// of SET mutations in the patch whose previous value MAC was not found locally
// (the signature of the LID migration).
//
// On a verified match it returns the orphaned entries to delete from the local
// MAC store and the corrected state. If recovery is not enabled, not possible,
// or finds no match, it returns ok=false and the caller must propagate the
// original error.
func (proc *Processor) tryLTHashRecovery(
	ctx context.Context,
	name WAPatchName,
	failedState HashState,
	keys ExpandedAppStateKeys,
	patch *waServerSync.SyncdPatch,
	missingPrevSETs int,
) (orphans []store.AppStateMutationMAC, fixedState HashState, ok bool) {
	applyEnabled := lidRecoveryApplyEnabled()
	diagDir := appStateDiagDir()
	if !applyEnabled && diagDir == "" {
		return
	}
	getter, hasGetter := proc.Store.AppState.(allAppStateMutationMACsGetter)
	if !hasGetter {
		proc.Log.Warnf("LTHash recovery for %s v%d skipped: store does not support listing mutation MACs", name, failedState.Version)
		return
	}
	candidates, err := getter.GetAllAppStateMutationMACs(ctx, string(name))
	if err != nil {
		proc.Log.Warnf("LTHash recovery for %s v%d skipped: failed to list mutation MACs: %v", name, failedState.Version, err)
		return
	}
	maxK := missingPrevSETs
	if maxK < 1 {
		// Verification can also fail without any missing previous value (e.g.
		// a stale local value MAC); a single-subtraction search is still worth
		// trying and remains sound because the result is MAC-verified.
		maxK = 1
	}
	start := time.Now()
	found, fixedHash, matched := findMissingPreviousValues(
		failedState, name, keys.SnapshotMAC, patch.GetSnapshotMAC(), candidates, maxK)
	elapsed := time.Since(start)

	diag := map[string]any{
		"ts":                 time.Now().UTC().Format(time.RFC3339Nano),
		"type":               "lthash_recovery_attempt",
		"collection":         string(name),
		"patch_version":      failedState.Version,
		"missing_prev_sets":  missingPrevSETs,
		"candidates":         len(candidates),
		"max_k":              maxK,
		"matched":            matched,
		"search_duration_ms": elapsed.Milliseconds(),
		"apply_enabled":      applyEnabled,
	}
	if matched {
		macs := make([]map[string]string, len(found))
		for i, f := range found {
			macs[i] = map[string]string{
				"index_mac": hex.EncodeToString(f.IndexMAC),
				"value_mac": hex.EncodeToString(f.ValueMAC),
			}
		}
		diag["orphans"] = macs
		proc.Log.Infof("LTHash recovery for %s v%d: found %d orphaned value MAC(s) in %v (apply=%t)",
			name, failedState.Version, len(found), elapsed, applyEnabled)
	} else {
		proc.Log.Warnf("LTHash recovery for %s v%d: no match among %d candidates (maxK=%d, %v)",
			name, failedState.Version, len(candidates), maxK, elapsed)
	}
	proc.appendDiagRecord(diag)

	if !matched || !applyEnabled {
		// Diagnostic-only mode: never touch state, let the original error propagate.
		return
	}
	fixedState = failedState
	fixedState.Hash = fixedHash
	return found, fixedState, true
}

// dumpPatchListForDiag writes the downloaded (still encrypted) snapshot and
// patches to the diagnostic directory. It is a no-op unless
// WHATSMEOW_APPSTATE_DIAG_DIR is set. Failures are logged and ignored; this
// must never affect the sync itself.
func (proc *Processor) dumpPatchListForDiag(list *PatchList) {
	dir := appStateDiagDir()
	if dir == "" {
		return
	}
	// Only the collection under investigation is dumped: regular_low is the
	// one whose full sync cannot verify on LID-migrated accounts. Dumping the
	// others costs real disk (a large account's regular snapshot is ~17 MB per
	// restart) without diagnostic value, since they verify normally.
	if list.Name != WAPatchRegularLow {
		return
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		proc.Log.Warnf("App state diag: failed to create %s: %v", dir, err)
		return
	}
	stamp := time.Now().UTC().Format("20060102T150405.000000000Z")
	writeBlob := func(suffix string, msg proto.Message, version uint64) {
		raw, err := proto.Marshal(msg)
		if err != nil {
			proc.Log.Warnf("App state diag: failed to marshal %s: %v", suffix, err)
			return
		}
		fname := fmt.Sprintf("%s-v%d-%s.%s.pb", list.Name, version, stamp, suffix)
		path := filepath.Join(dir, fname)
		if err = os.WriteFile(path, raw, 0o600); err != nil {
			proc.Log.Warnf("App state diag: failed to write %s: %v", path, err)
			return
		}
		proc.appendDiagRecord(map[string]any{
			"ts":         time.Now().UTC().Format(time.RFC3339Nano),
			"type":       "dump",
			"collection": string(list.Name),
			"kind":       suffix,
			"version":    version,
			"file":       fname,
			"size":       len(raw),
		})
	}
	if list.Snapshot != nil {
		writeBlob("snapshot", list.Snapshot, list.Snapshot.GetVersion().GetVersion())
	}
	for _, patch := range list.Patches {
		writeBlob("patch", patch, patch.GetVersion().GetVersion())
	}
}

func (proc *Processor) appendDiagRecord(record map[string]any) {
	dir := appStateDiagDir()
	if dir == "" {
		return
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return
	}
	line, err := json.Marshal(record)
	if err != nil {
		return
	}
	f, err := os.OpenFile(filepath.Join(dir, "appstate-diag.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		proc.Log.Warnf("App state diag: failed to open log: %v", err)
		return
	}
	defer f.Close()
	_, _ = f.Write(append(line, '\n'))
}

// trySnapshotDedup recomputes a full snapshot's LTHash counting each index MAC
// once (the last record wins), which is how WhatsApp Web hashes snapshots
// (WAWebSyncdAntiTampering). It is only called after normal verification has
// failed. The result is recorded in diagnostic mode and is only returned as
// usable (ok=true) when the snapshot MAC verifies and
// WHATSMEOW_APPSTATE_LID_RECOVERY=1 is set.
func (proc *Processor) trySnapshotDedup(
	ctx context.Context,
	name WAPatchName,
	initialState HashState,
	ss *waServerSync.SyncdSnapshot,
) (dedupState HashState, ok bool) {
	applyEnabled := lidRecoveryApplyEnabled()
	if !applyEnabled && appStateDiagDir() == "" {
		return
	}
	records := ss.GetRecords()
	lastByIndex := make(map[string]int, len(records))
	for i, record := range records {
		lastByIndex[string(record.GetIndex().GetBlob())] = i
	}
	added := make([][]byte, 0, len(lastByIndex))
	for i, record := range records {
		if lastByIndex[string(record.GetIndex().GetBlob())] != i {
			continue
		}
		value := record.GetValue().GetBlob()
		if len(value) < 32 {
			return
		}
		added = append(added, value[len(value)-32:])
	}
	dedupState = initialState
	dedupState.Version = ss.GetVersion().GetVersion()
	lthash.WAPatchIntegrity.SubtractThenAddInPlace(dedupState.Hash[:], nil, added)
	_, err := proc.validateSnapshotMAC(ctx, name, dedupState, ss.GetKeyID().GetID(), ss.GetMac())
	matched := err == nil

	proc.appendDiagRecord(map[string]any{
		"ts":                time.Now().UTC().Format(time.RFC3339Nano),
		"type":              "snapshot_dedup_attempt",
		"collection":        string(name),
		"snapshot_version":  dedupState.Version,
		"records":           len(records),
		"duplicate_records": len(records) - len(lastByIndex),
		"matched":           matched,
		"apply_enabled":     applyEnabled,
	})
	if matched {
		proc.Log.Infof("Snapshot dedup for %s v%d verified (%d duplicate records, apply=%t)",
			name, dedupState.Version, len(records)-len(lastByIndex), applyEnabled)
	} else {
		proc.Log.Warnf("Snapshot dedup for %s v%d did not verify (%d duplicate records)",
			name, dedupState.Version, len(records)-len(lastByIndex))
	}
	return dedupState, matched && applyEnabled
}
