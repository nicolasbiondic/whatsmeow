package appstate

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"go.mau.fi/whatsmeow/proto/waServerSync"
)

// buildTestSnapshot encodes the given patch's mutations as snapshot records
// and returns a snapshot whose MAC deliberately does NOT verify.
func buildTestSnapshot(t *testing.T, env *lidWriteEnv, version uint64, patchInfo PatchInfo) *waServerSync.SyncdSnapshot {
	t.Helper()
	encoded, err := env.proc.EncodePatch(context.Background(), env.newKeyID, HashState{}, patchInfo)
	if err != nil {
		t.Fatalf("EncodePatch: %v", err)
	}
	var patch waServerSync.SyncdPatch
	if err = proto.Unmarshal(encoded, &patch); err != nil {
		t.Fatalf("unmarshal patch: %v", err)
	}
	records := make([]*waServerSync.SyncdRecord, len(patch.GetMutations()))
	for i, m := range patch.GetMutations() {
		records[i] = m.GetRecord()
	}
	return &waServerSync.SyncdSnapshot{
		Version: &waServerSync.SyncdVersion{Version: proto.Uint64(version)},
		Records: records,
		Mac:     randBytes(t, 32), // can never verify
		KeyID:   &waServerSync.KeyId{ID: env.newKeyID},
	}
}

// A snapshot whose MAC does not verify but whose records MAC-verify and
// contain LID-indexed chat actions is adopted with the plain-sum hash when
// the context carries the bootstrap flag. Fails without change B.
func TestSnapshotBootstrapAcceptsUnverifiedLIDSnapshot(t *testing.T) {
	t.Setenv(companionToleranceEnvVar, "")
	diagDir := t.TempDir()
	t.Setenv(diagDirEnvVar, diagDir)
	env := newLIDWriteEnv(t, false)
	ss := buildTestSnapshot(t, env, 222, BuildArchive(env.lid, true, time.Now(), nil))

	list := &PatchList{Name: WAPatchRegularLow, Snapshot: ss}
	mutations, got, err := env.proc.DecodePatches(WithSnapshotBootstrap(context.Background()), list, HashState{}, true)
	if err != nil {
		t.Fatalf("DecodePatches with the bootstrap flag: %v", err)
	}
	if got.Version != 222 {
		t.Fatalf("version = %d, want 222", got.Version)
	}
	if len(mutations) != len(ss.GetRecords()) {
		t.Fatalf("decoded %d mutations, want %d", len(mutations), len(ss.GetRecords()))
	}
	// The adopted hash must be the plain sum of the records' value MACs.
	var valueMACs [][]byte
	for _, record := range ss.GetRecords() {
		blob := record.GetValue().GetBlob()
		valueMACs = append(valueMACs, blob[len(blob)-32:])
	}
	if want := buildState(222, valueMACs); got.Hash != want.Hash {
		t.Error("adopted hash is not the plain sum of the records")
	}
	// The LID records prove the migration: the cache must now be set so later
	// primary patches with diverged hashes are tolerated too.
	env.proc.lidMigratedCacheLock.Lock()
	migrated := env.proc.lidMigratedCache[WAPatchRegularLow]
	env.proc.lidMigratedCacheLock.Unlock()
	if !migrated {
		t.Error("collection was not marked LID-migrated after the bootstrap")
	}
	diag, err := os.ReadFile(filepath.Join(diagDir, "appstate-diag.jsonl"))
	if err != nil || !strings.Contains(string(diag), `"type":"snapshot_bootstrap_tolerated"`) {
		t.Errorf("diag log lacks snapshot_bootstrap_tolerated (err=%v): %s", err, diag)
	}
}

// The same snapshot without the context flag must keep failing: with prior
// local state (or a capture error) nothing changes.
func TestSnapshotBootstrapRejectedWithoutFlag(t *testing.T) {
	t.Setenv(companionToleranceEnvVar, "")
	env := newLIDWriteEnv(t, false)
	ss := buildTestSnapshot(t, env, 222, BuildArchive(env.lid, true, time.Now(), nil))
	list := &PatchList{Name: WAPatchRegularLow, Snapshot: ss}
	if _, _, err := env.proc.DecodePatches(context.Background(), list, HashState{}, true); !errors.Is(err, ErrMismatchingLTHash) {
		t.Fatalf("without the flag: err = %v, want ErrMismatchingLTHash", err)
	}
}

// A snapshot without any LID-indexed chat action (e.g. a tampered or
// non-migrated collection) is rejected even with the flag, and nothing is
// stored.
func TestSnapshotBootstrapRejectedWithoutLIDRecords(t *testing.T) {
	t.Setenv(companionToleranceEnvVar, "")
	env := newLIDWriteEnv(t, false)
	ss := buildTestSnapshot(t, env, 222, BuildArchive(env.pn, true, time.Now(), nil))
	list := &PatchList{Name: WAPatchRegularLow, Snapshot: ss}
	if _, _, err := env.proc.DecodePatches(WithSnapshotBootstrap(context.Background()), list, HashState{}, true); !errors.Is(err, ErrMismatchingLTHash) {
		t.Fatalf("PN-only snapshot with the flag: err = %v, want ErrMismatchingLTHash", err)
	}
	if env.appState.versions[string(WAPatchRegularLow)] != 100 {
		t.Errorf("rejected bootstrap stored version %d", env.appState.versions[string(WAPatchRegularLow)])
	}
}

// A record with a bad content MAC is rejected even with the flag: the
// records of a bootstrapped snapshot are still fully MAC-verified.
func TestSnapshotBootstrapRejectsBadContentMAC(t *testing.T) {
	t.Setenv(companionToleranceEnvVar, "")
	env := newLIDWriteEnv(t, false)
	ss := buildTestSnapshot(t, env, 222, BuildArchive(env.lid, true, time.Now(), nil))
	ss.GetRecords()[0].GetValue().GetBlob()[0] ^= 0xff
	list := &PatchList{Name: WAPatchRegularLow, Snapshot: ss}
	if _, _, err := env.proc.DecodePatches(WithSnapshotBootstrap(context.Background()), list, HashState{}, true); !errors.Is(err, ErrMismatchingLTHash) {
		t.Fatalf("snapshot with a bad content MAC: err = %v, want the original ErrMismatchingLTHash", err)
	}
	if env.appState.versions[string(WAPatchRegularLow)] != 100 {
		t.Errorf("rejected bootstrap stored version %d", env.appState.versions[string(WAPatchRegularLow)])
	}
}
