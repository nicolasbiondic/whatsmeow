package appstate

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"os"
	"strings"
	"testing"

	"go.mau.fi/whatsmeow/appstate/lthash"
	"go.mau.fi/whatsmeow/proto/waServerSync"
	"go.mau.fi/whatsmeow/store"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
)

func randBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatalf("rand: %v", err)
	}
	return b
}

// buildState sums the given value MACs into a hash using the real LTHash
// implementation from go.mau.fi/whatsmeow/appstate/lthash.
func buildState(version uint64, valueMACs [][]byte) HashState {
	var hs HashState
	hs.Version = version
	lthash.WAPatchIntegrity.SubtractThenAddInPlace(hs.Hash[:], nil, valueMACs)
	return hs
}

func makeCandidates(t *testing.T, n int) []store.AppStateMutationMAC {
	t.Helper()
	out := make([]store.AppStateMutationMAC, n)
	for i := range out {
		out[i] = store.AppStateMutationMAC{IndexMAC: randBytes(t, 32), ValueMAC: randBytes(t, 32)}
	}
	return out
}

func valueMACs(cands []store.AppStateMutationMAC) [][]byte {
	out := make([][]byte, len(cands))
	for i, c := range cands {
		out[i] = c.ValueMAC
	}
	return out
}

// Scenario: the true server state at some version does NOT contain the
// orphan's contribution (the primary subtracted it during the PN→LID
// migration), but the locally computed state still does. The recovery must
// identify the orphan by brute force against the snapshot MAC.
func TestFindMissingPreviousValues_SingleOrphan(t *testing.T) {
	const version = 1636
	name := WAPatchRegularLow
	key := randBytes(t, 32)

	candidates := makeCandidates(t, 500)
	orphan := candidates[137]

	// True server hash: all candidates except the orphan.
	var trueVMs [][]byte
	for i, c := range candidates {
		if i != 137 {
			trueVMs = append(trueVMs, c.ValueMAC)
		}
	}
	trueState := buildState(version, trueVMs)
	expectedMAC := trueState.generateSnapshotMAC(name, key)

	// Locally computed (failing) state: includes the orphan.
	localState := buildState(version, valueMACs(candidates))

	found, fixedHash, ok := findMissingPreviousValues(localState, name, key, expectedMAC, candidates, 1)
	if !ok {
		t.Fatal("expected recovery to find the orphan")
	}
	if len(found) != 1 || string(found[0].IndexMAC) != string(orphan.IndexMAC) {
		t.Fatalf("recovered wrong entry: %x", found[0].IndexMAC)
	}
	if fixedHash != trueState.Hash {
		t.Fatal("fixed hash does not match the true server hash")
	}
}

func TestFindMissingPreviousValues_TwoOrphans(t *testing.T) {
	const version = 2000
	name := WAPatchRegularLow
	key := randBytes(t, 32)

	candidates := makeCandidates(t, 120)
	skip := map[int]bool{11: true, 97: true}
	var trueVMs [][]byte
	for i, c := range candidates {
		if !skip[i] {
			trueVMs = append(trueVMs, c.ValueMAC)
		}
	}
	trueState := buildState(version, trueVMs)
	expectedMAC := trueState.generateSnapshotMAC(name, key)
	localState := buildState(version, valueMACs(candidates))

	// k=1 must not find anything (two entries are missing).
	if _, _, ok := findMissingPreviousValues(localState, name, key, expectedMAC, candidates, 1); ok {
		t.Fatal("k=1 search should not succeed when two entries are missing")
	}
	found, fixedHash, ok := findMissingPreviousValues(localState, name, key, expectedMAC, candidates, 2)
	if !ok {
		t.Fatal("expected k=2 recovery to find both orphans")
	}
	if len(found) != 2 {
		t.Fatalf("expected 2 recovered entries, got %d", len(found))
	}
	got := map[string]bool{string(found[0].IndexMAC): true, string(found[1].IndexMAC): true}
	if !got[string(candidates[11].IndexMAC)] || !got[string(candidates[97].IndexMAC)] {
		t.Fatal("recovered wrong pair")
	}
	if fixedHash != trueState.Hash {
		t.Fatal("fixed hash does not match the true server hash")
	}
}

// If the orphan is not among the candidates (e.g. the hidden entry was never
// delivered to this device), the search must fail cleanly instead of
// "recovering" something wrong.
func TestFindMissingPreviousValues_NoMatch(t *testing.T) {
	const version = 10
	name := WAPatchRegularLow
	key := randBytes(t, 32)

	candidates := makeCandidates(t, 300)
	hiddenOrphan := randBytes(t, 32) // not in candidates

	allVMs := append(valueMACs(candidates), hiddenOrphan)
	localState := buildState(version, allVMs)
	trueState := buildState(version, valueMACs(candidates))
	expectedMAC := trueState.generateSnapshotMAC(name, key)

	// Sanity: subtracting the hidden orphan would verify, but it's not a candidate.
	if _, _, ok := findMissingPreviousValues(localState, name, key, expectedMAC, candidates, 2); ok {
		t.Fatal("search must not succeed when the orphan is not among the candidates")
	}
}

// With a garbage expected MAC nothing may ever match (false positive check).
func TestFindMissingPreviousValues_GarbageMAC(t *testing.T) {
	candidates := makeCandidates(t, 300)
	localState := buildState(5, valueMACs(candidates))
	if _, _, ok := findMissingPreviousValues(localState, WAPatchRegularLow, randBytes(t, 32), randBytes(t, 32), candidates, 2); ok {
		t.Fatal("search matched a random MAC; HMAC verification is broken")
	}
}

type fakeAppStateStore struct {
	store.AppStateStore
	macs []store.AppStateMutationMAC
}

func (f *fakeAppStateStore) GetAllAppStateMutationMACs(ctx context.Context, name string) ([]store.AppStateMutationMAC, error) {
	return f.macs, nil
}

// End-to-end test of the tryLTHashRecovery hook with the env var gates.
func TestTryLTHashRecovery_ApplyAndDryRun(t *testing.T) {
	const version = 1636
	name := WAPatchRegularLow
	key := randBytes(t, 32)

	candidates := makeCandidates(t, 200)
	orphan := candidates[42]
	var trueVMs [][]byte
	for i, c := range candidates {
		if i != 42 {
			trueVMs = append(trueVMs, c.ValueMAC)
		}
	}
	trueState := buildState(version, trueVMs)
	localState := buildState(version, valueMACs(candidates))
	patch := &waServerSync.SyncdPatch{SnapshotMAC: trueState.generateSnapshotMAC(name, key)}
	keys := ExpandedAppStateKeys{SnapshotMAC: key}

	proc := &Processor{
		Store: &store.Device{AppState: &fakeAppStateStore{macs: candidates}},
		Log:   waLog.Noop,
	}

	// Disabled: no recovery at all.
	t.Setenv(lidRecoveryEnvVar, "")
	t.Setenv(diagDirEnvVar, "")
	if _, _, ok := proc.tryLTHashRecovery(context.Background(), name, localState, keys, patch, 1); ok {
		t.Fatal("recovery must be a no-op when disabled")
	}

	// Diagnostic mode: search runs and is recorded, but is not applied.
	t.Setenv(diagDirEnvVar, t.TempDir())
	if _, _, ok := proc.tryLTHashRecovery(context.Background(), name, localState, keys, patch, 1); ok {
		t.Fatal("recovery must not be applied in diagnostic-only mode")
	}

	// Apply mode.
	t.Setenv(lidRecoveryEnvVar, "1")
	orphans, fixedState, ok := proc.tryLTHashRecovery(context.Background(), name, localState, keys, patch, 1)
	if !ok {
		t.Fatal("expected recovery to apply")
	}
	if len(orphans) != 1 || string(orphans[0].IndexMAC) != string(orphan.IndexMAC) {
		t.Fatal("recovered wrong orphan")
	}
	if fixedState.Hash != trueState.Hash || fixedState.Version != version {
		t.Fatal("fixed state mismatch")
	}
}

// Worst-case benchmarks with the real candidate count of the 964 tenant
// (1702 regular_low entries): no match is found, forcing a full scan.
func benchmarkNoMatch(b *testing.B, maxK int) {
	candidates := make([]store.AppStateMutationMAC, 1702)
	for i := range candidates {
		im := make([]byte, 32)
		vm := make([]byte, 32)
		rand.Read(im)
		rand.Read(vm)
		candidates[i] = store.AppStateMutationMAC{IndexMAC: im, ValueMAC: vm}
	}
	var state HashState
	state.Version = 1636
	key := make([]byte, 32)
	mac := make([]byte, 32)
	rand.Read(key)
	rand.Read(mac)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, _, ok := findMissingPreviousValues(state, WAPatchRegularLow, key, mac, candidates, maxK); ok {
			b.Fatal("unexpected match")
		}
	}
}

func BenchmarkFindMissingPreviousValues_K1_N1702(b *testing.B) { benchmarkNoMatch(b, 1) }
func BenchmarkFindMissingPreviousValues_K2_N1702(b *testing.B) { benchmarkNoMatch(b, 2) }

// A snapshot with duplicate index MACs must verify when each index is counted
// once (last record wins), as WhatsApp Web does, and only be applied when the
// apply gate is set.
func TestTrySnapshotDedup(t *testing.T) {
	const version = 1700
	name := WAPatchRegularLow
	keyID := []byte("test-key")
	keys := ExpandedAppStateKeys{SnapshotMAC: randBytes(t, 32)}

	record := func(index, valueMAC []byte) *waServerSync.SyncdRecord {
		return &waServerSync.SyncdRecord{
			Index: &waServerSync.SyncdIndex{Blob: index},
			Value: &waServerSync.SyncdValue{Blob: append(randBytes(t, 48), valueMAC...)},
		}
	}
	dupIndex := randBytes(t, 32)
	staleVM, freshVM := randBytes(t, 32), randBytes(t, 32)
	var records []*waServerSync.SyncdRecord
	var trueVMs [][]byte
	records = append(records, record(dupIndex, staleVM))
	for range 50 {
		vm := randBytes(t, 32)
		records = append(records, record(randBytes(t, 32), vm))
		trueVMs = append(trueVMs, vm)
	}
	records = append(records, record(dupIndex, freshVM))
	trueVMs = append(trueVMs, freshVM)

	trueState := buildState(version, trueVMs)
	ss := &waServerSync.SyncdSnapshot{
		Version: &waServerSync.SyncdVersion{Version: proto.Uint64(version)},
		Records: records,
		Mac:     trueState.generateSnapshotMAC(name, keys.SnapshotMAC),
		KeyID:   &waServerSync.KeyId{ID: keyID},
	}
	proc := &Processor{
		keyCache: map[string]ExpandedAppStateKeys{base64.RawStdEncoding.EncodeToString(keyID): keys},
		Store:    &store.Device{},
		Log:      waLog.Noop,
	}

	t.Setenv(lidRecoveryEnvVar, "")
	t.Setenv(diagDirEnvVar, "")
	if _, ok := proc.trySnapshotDedup(context.Background(), name, HashState{}, ss); ok {
		t.Fatal("dedup must be a no-op when disabled")
	}
	t.Setenv(diagDirEnvVar, t.TempDir())
	if _, ok := proc.trySnapshotDedup(context.Background(), name, HashState{}, ss); ok {
		t.Fatal("dedup must not be applied in diagnostic-only mode")
	}
	t.Setenv(lidRecoveryEnvVar, "1")
	got, ok := proc.trySnapshotDedup(context.Background(), name, HashState{}, ss)
	if !ok || got.Hash != trueState.Hash || got.Version != version {
		t.Fatal("expected deduplicated snapshot to verify and apply")
	}
	ss.Mac = randBytes(t, 32)
	if _, ok := proc.trySnapshotDedup(context.Background(), name, HashState{}, ss); ok {
		t.Fatal("dedup must not apply when the MAC does not verify")
	}
}

// Diagnostic dumps are restricted to regular_low: on a large account the
// regular snapshot alone costs ~17 MB of disk per restart, and only
// regular_low's full sync is under investigation. Fails without the
// collection check in dumpPatchListForDiag.
func TestDumpPatchListForDiagOnlyRegularLow(t *testing.T) {
	proc := &Processor{Store: &store.Device{}, Log: waLog.Noop}
	dir := t.TempDir()
	t.Setenv(diagDirEnvVar, dir)
	t.Setenv(diagDumpsEnvVar, "1")

	list := func(name WAPatchName) *PatchList {
		return &PatchList{
			Name: name,
			Snapshot: &waServerSync.SyncdSnapshot{
				Version: &waServerSync.SyncdVersion{Version: proto.Uint64(7)},
			},
			Patches: []*waServerSync.SyncdPatch{{
				Version: &waServerSync.SyncdVersion{Version: proto.Uint64(8)},
			}},
		}
	}
	countPB := func() int {
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for _, e := range entries {
			if strings.HasSuffix(e.Name(), ".pb") {
				n++
			}
		}
		return n
	}

	for _, name := range []WAPatchName{WAPatchRegular, WAPatchRegularHigh, WAPatchCriticalBlock, WAPatchCriticalUnblockLow} {
		proc.dumpPatchListForDiag(list(name))
		if got := countPB(); got != 0 {
			t.Fatalf("collection %s wrote %d dump files; only regular_low may be dumped", name, got)
		}
	}
	proc.dumpPatchListForDiag(list(WAPatchRegularLow))
	if got := countPB(); got != 2 {
		t.Fatalf("regular_low should have dumped snapshot+patch (2 files), got %d", got)
	}
}
