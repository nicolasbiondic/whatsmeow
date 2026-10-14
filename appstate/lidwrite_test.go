package appstate

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"reflect"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"go.mau.fi/whatsmeow/appstate/lthash"
	"go.mau.fi/whatsmeow/proto/waServerSync"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// ---- fakes -----------------------------------------------------------------

type memAppStateStore struct {
	versions map[string]uint64
	hashes   map[string][128]byte
	macs     map[string]map[[32]byte][]byte
}

func newMemAppStateStore() *memAppStateStore {
	return &memAppStateStore{
		versions: make(map[string]uint64),
		hashes:   make(map[string][128]byte),
		macs:     make(map[string]map[[32]byte][]byte),
	}
}

func (f *memAppStateStore) macsFor(name string) map[[32]byte][]byte {
	if f.macs[name] == nil {
		f.macs[name] = make(map[[32]byte][]byte)
	}
	return f.macs[name]
}

func (f *memAppStateStore) PutAppStateVersion(ctx context.Context, name string, version uint64, hash [128]byte) error {
	f.versions[name] = version
	f.hashes[name] = hash
	return nil
}

func (f *memAppStateStore) GetAppStateVersion(ctx context.Context, name string) (uint64, [128]byte, error) {
	return f.versions[name], f.hashes[name], nil
}

func (f *memAppStateStore) DeleteAppStateVersion(ctx context.Context, name string) error {
	delete(f.versions, name)
	delete(f.hashes, name)
	return nil
}

func (f *memAppStateStore) PutAppStateMutationMACs(ctx context.Context, name string, version uint64, mutations []store.AppStateMutationMAC) error {
	m := f.macsFor(name)
	for _, mut := range mutations {
		if len(mut.IndexMAC) != 32 {
			return fmt.Errorf("invalid index MAC length %d", len(mut.IndexMAC))
		}
		m[*(*[32]byte)(mut.IndexMAC)] = mut.ValueMAC
	}
	return nil
}

func (f *memAppStateStore) DeleteAppStateMutationMACs(ctx context.Context, name string, indexMACs [][]byte) error {
	m := f.macsFor(name)
	for _, mac := range indexMACs {
		if len(mac) == 32 {
			delete(m, *(*[32]byte)(mac))
		}
	}
	return nil
}

func (f *memAppStateStore) GetAppStateMutationMAC(ctx context.Context, name string, indexMAC []byte) ([]byte, error) {
	if len(indexMAC) != 32 {
		return nil, nil
	}
	return f.macsFor(name)[*(*[32]byte)(indexMAC)], nil
}

func (f *memAppStateStore) GetAllAppStateMutationMACs(ctx context.Context, name string) ([]store.AppStateMutationMAC, error) {
	var out []store.AppStateMutationMAC
	for idx, val := range f.macsFor(name) {
		idxCopy := idx
		out = append(out, store.AppStateMutationMAC{IndexMAC: idxCopy[:], ValueMAC: val})
	}
	return out, nil
}

type fakeKeyStore struct {
	keys  map[string]*store.AppStateSyncKey
	order [][]byte // newest first
}

func newFakeKeyStore() *fakeKeyStore {
	return &fakeKeyStore{keys: make(map[string]*store.AppStateSyncKey)}
}

func (f *fakeKeyStore) addKey(id, data []byte) {
	f.keys[string(id)] = &store.AppStateSyncKey{Data: data}
	f.order = append([][]byte{id}, f.order...)
}

func (f *fakeKeyStore) PutAppStateSyncKey(ctx context.Context, id []byte, key store.AppStateSyncKey) error {
	f.keys[string(id)] = &key
	f.order = append([][]byte{id}, f.order...)
	return nil
}

func (f *fakeKeyStore) GetAppStateSyncKey(ctx context.Context, id []byte) (*store.AppStateSyncKey, error) {
	return f.keys[string(id)], nil
}

func (f *fakeKeyStore) GetLatestAppStateSyncKeyID(ctx context.Context) ([]byte, error) {
	if len(f.order) == 0 {
		return nil, nil
	}
	return f.order[0], nil
}

func (f *fakeKeyStore) GetAllAppStateSyncKeys(ctx context.Context) ([]*store.AppStateSyncKey, error) {
	out := make([]*store.AppStateSyncKey, 0, len(f.order))
	for _, id := range f.order {
		out = append(out, f.keys[string(id)])
	}
	return out, nil
}

func (f *fakeKeyStore) GetAllAppStateSyncKeyIDs(ctx context.Context) ([][]byte, error) {
	return f.order, nil
}

type fakeLIDStore struct {
	lidToPN map[string]string
	pnToLID map[string]string
}

func newFakeLIDStore() *fakeLIDStore {
	return &fakeLIDStore{lidToPN: make(map[string]string), pnToLID: make(map[string]string)}
}

func (f *fakeLIDStore) PutLIDMapping(ctx context.Context, lid, pn types.JID) error {
	f.lidToPN[lid.User] = pn.User
	f.pnToLID[pn.User] = lid.User
	return nil
}

func (f *fakeLIDStore) PutManyLIDMappings(ctx context.Context, mappings []store.LIDMapping) error {
	for _, m := range mappings {
		f.lidToPN[m.LID.User] = m.PN.User
		f.pnToLID[m.PN.User] = m.LID.User
	}
	return nil
}

func (f *fakeLIDStore) GetPNForLID(ctx context.Context, lid types.JID) (types.JID, error) {
	if pn, ok := f.lidToPN[lid.User]; ok {
		return types.JID{User: pn, Server: types.DefaultUserServer}, nil
	}
	return types.JID{}, nil
}

func (f *fakeLIDStore) GetLIDForPN(ctx context.Context, pn types.JID) (types.JID, error) {
	if lid, ok := f.pnToLID[pn.User]; ok {
		return types.JID{User: lid, Server: types.HiddenUserServer}, nil
	}
	return types.JID{}, nil
}

func (f *fakeLIDStore) GetManyLIDsForPNs(ctx context.Context, pns []types.JID) (map[types.JID]types.JID, error) {
	out := make(map[types.JID]types.JID)
	for _, pn := range pns {
		if lid, ok := f.pnToLID[pn.User]; ok {
			out[pn] = types.JID{User: lid, Server: types.HiddenUserServer}
		}
	}
	return out, nil
}

func (f *fakeLIDStore) GetAllLIDMappings(ctx context.Context) ([]store.LIDMapping, error) {
	var out []store.LIDMapping
	for lid, pn := range f.lidToPN {
		out = append(out, store.LIDMapping{
			LID: types.JID{User: lid, Server: types.HiddenUserServer},
			PN:  types.JID{User: pn, Server: types.DefaultUserServer},
		})
	}
	return out, nil
}

// ---- environment -----------------------------------------------------------

type lidWriteEnv struct {
	proc     *Processor
	appState *memAppStateStore
	keyStore *fakeKeyStore
	lids     *fakeLIDStore

	oldKeyID []byte
	newKeyID []byte

	lid types.JID
	pn  types.JID

	// seeded value MACs
	vArchivePN  []byte
	vPinPN      []byte
	vArchiveLID []byte

	state HashState
}

func indexMACFor(t *testing.T, keyData []byte, index []string) []byte {
	t.Helper()
	indexBytes, err := json.Marshal(index)
	if err != nil {
		t.Fatalf("marshal index: %v", err)
	}
	keys := expandAppStateKeys(keyData)
	return concatAndHMAC(sha256.New, keys.Index, indexBytes)
}

// newLIDWriteEnv builds an environment shaped like the real "964" account:
// two app state keys (old + new/latest), a LID↔PN mapping, an archive and a
// pin_v1 entry under the PN index written with the OLD key, and (if
// migrated) an archive entry under the LID index written with the NEW key.
func newLIDWriteEnv(t *testing.T, migrated bool) *lidWriteEnv {
	t.Helper()
	env := &lidWriteEnv{
		appState: newMemAppStateStore(),
		keyStore: newFakeKeyStore(),
		lids:     newFakeLIDStore(),
		oldKeyID: []byte{0, 0, 0, 0, 0xf3, 0xa0},
		newKeyID: []byte{0, 0, 0, 0, 0xf3, 0xa6},
		lid:      types.JID{User: "137929267658756", Server: types.HiddenUserServer},
		pn:       types.JID{User: "51905448512", Server: types.DefaultUserServer},
	}
	oldKeyData := randBytes(t, 32)
	newKeyData := randBytes(t, 32)
	env.keyStore.addKey(env.oldKeyID, oldKeyData) // older
	env.keyStore.addKey(env.newKeyID, newKeyData) // newest (added last = first in order)
	if err := env.lids.PutLIDMapping(context.Background(), env.lid, env.pn); err != nil {
		t.Fatalf("put lid mapping: %v", err)
	}

	env.vArchivePN = randBytes(t, 32)
	env.vPinPN = randBytes(t, 32)
	seed := []store.AppStateMutationMAC{
		{IndexMAC: indexMACFor(t, oldKeyData, []string{IndexArchive, env.pn.String()}), ValueMAC: env.vArchivePN},
		{IndexMAC: indexMACFor(t, oldKeyData, []string{IndexPin, env.pn.String()}), ValueMAC: env.vPinPN},
	}
	valueMACs := [][]byte{env.vArchivePN, env.vPinPN}
	if migrated {
		env.vArchiveLID = randBytes(t, 32)
		seed = append(seed, store.AppStateMutationMAC{
			IndexMAC: indexMACFor(t, newKeyData, []string{IndexArchive, env.lid.String()}),
			ValueMAC: env.vArchiveLID,
		})
		valueMACs = append(valueMACs, env.vArchiveLID)
	}
	ctx := context.Background()
	if err := env.appState.PutAppStateMutationMACs(ctx, string(WAPatchRegularLow), 100, seed); err != nil {
		t.Fatalf("seed MACs: %v", err)
	}
	env.state = buildState(100, valueMACs)
	if err := env.appState.PutAppStateVersion(ctx, string(WAPatchRegularLow), env.state.Version, env.state.Hash); err != nil {
		t.Fatalf("seed version: %v", err)
	}

	device := &store.Device{
		AppState:     env.appState,
		AppStateKeys: env.keyStore,
		LIDs:         env.lids,
	}
	env.proc = NewProcessor(device, waLog.Noop)
	return env
}

func mutationSummary(m MutationInfo) string {
	return fmt.Sprintf("%s %v key=%x", m.Operation, m.Index, m.KeyID)
}

// ---- tests -----------------------------------------------------------------

// On a migrated account, archiving the LID chat must produce the WhatsApp Web
// shaped patch: SET(archive, LID) + SET(pin, LID) + REMOVE(archive, PN) +
// REMOVE(pin, PN), with the REMOVEs under the OLD key that matches the stored
// PN rows. Fails without the lidwrite/encode changes.
func TestExpandPatchForLID_Migrated(t *testing.T) {
	env := newLIDWriteEnv(t, true)
	ctx := context.Background()

	patch := BuildArchive(env.lid, true, time.Now(), nil)
	expanded, err := env.proc.ExpandPatchForLID(ctx, patch)
	if err != nil {
		t.Fatalf("ExpandPatchForLID: %v", err)
	}
	if len(expanded.Mutations) != 4 {
		for _, m := range expanded.Mutations {
			t.Logf("mutation: %s", mutationSummary(m))
		}
		t.Fatalf("expected 4 mutations, got %d", len(expanded.Mutations))
	}
	var removes []MutationInfo
	for _, m := range expanded.Mutations {
		switch m.Operation {
		case waServerSync.SyncdMutation_SET:
			if m.Index[1] != env.lid.String() {
				t.Errorf("SET mutation index is not the LID: %v", m.Index)
			}
			if len(m.KeyID) != 0 {
				t.Errorf("SET mutation should use the patch key, got %x", m.KeyID)
			}
		case waServerSync.SyncdMutation_REMOVE:
			removes = append(removes, m)
		}
	}
	if len(removes) != 2 {
		t.Fatalf("expected 2 REMOVE mutations, got %d", len(removes))
	}
	seen := map[string]bool{}
	for _, m := range removes {
		seen[m.Index[0]] = true
		if m.Index[1] != env.pn.String() {
			t.Errorf("REMOVE index is not the PN: %v", m.Index)
		}
		if string(m.KeyID) != string(env.oldKeyID) {
			t.Errorf("REMOVE for %v should use old key %x, got %x", m.Index, env.oldKeyID, m.KeyID)
		}
		if m.Value == nil {
			t.Errorf("REMOVE for %v has nil value", m.Index)
		}
	}
	if !seen[IndexArchive] || !seen[IndexPin] {
		t.Errorf("expected REMOVEs for archive and pin_v1, got %v", seen)
	}
}

// Re-expanding the original patch must give the same result (no duplicated
// REMOVEs), which is what the client's retry path relies on.
func TestExpandPatchForLID_Idempotent(t *testing.T) {
	env := newLIDWriteEnv(t, true)
	ctx := context.Background()
	patch := BuildArchive(env.lid, true, time.Now(), nil)
	first, err := env.proc.ExpandPatchForLID(ctx, patch)
	if err != nil {
		t.Fatalf("first expand: %v", err)
	}
	second, err := env.proc.ExpandPatchForLID(ctx, patch)
	if err != nil {
		t.Fatalf("second expand: %v", err)
	}
	if len(first.Mutations) != len(second.Mutations) {
		t.Fatalf("expansion not idempotent: %d vs %d mutations", len(first.Mutations), len(second.Mutations))
	}
	if len(patch.Mutations) != 2 {
		t.Fatalf("original patch was mutated: %d mutations", len(patch.Mutations))
	}
}

// A PN-targeted write on a migrated account must be rewritten to the LID
// index (WA Web getChatJidMutationIndexForChat) with the same PN cleanup.
func TestExpandPatchForLID_PNTargetRewritten(t *testing.T) {
	env := newLIDWriteEnv(t, true)
	ctx := context.Background()
	patch := BuildArchive(env.pn, false, time.Now(), nil)
	expanded, err := env.proc.ExpandPatchForLID(ctx, patch)
	if err != nil {
		t.Fatalf("ExpandPatchForLID: %v", err)
	}
	// archive=false → single SET + one REMOVE (archive PN twin)
	if len(expanded.Mutations) != 2 {
		t.Fatalf("expected 2 mutations, got %d", len(expanded.Mutations))
	}
	set, rem := expanded.Mutations[0], expanded.Mutations[1]
	if set.Operation != waServerSync.SyncdMutation_SET || set.Index[1] != env.lid.String() {
		t.Errorf("expected SET rewritten to LID index, got %s", mutationSummary(set))
	}
	if rem.Operation != waServerSync.SyncdMutation_REMOVE || rem.Index[1] != env.pn.String() || string(rem.KeyID) != string(env.oldKeyID) {
		t.Errorf("expected REMOVE of PN twin under old key, got %s", mutationSummary(rem))
	}
}

// Non-migrated account (LID mappings and PN rows exist, but zero LID-indexed
// entries — the shape of the "Notificaciones" line): behavior must be
// exactly today's, i.e. the patch is returned unchanged.
func TestExpandPatchForLID_NotMigratedUnchanged(t *testing.T) {
	env := newLIDWriteEnv(t, false)
	ctx := context.Background()
	for _, target := range []types.JID{env.pn, env.lid} {
		patch := BuildArchive(target, true, time.Now(), nil)
		expanded, err := env.proc.ExpandPatchForLID(ctx, patch)
		if err != nil {
			t.Fatalf("ExpandPatchForLID(%s): %v", target, err)
		}
		if !reflect.DeepEqual(expanded.Mutations, patch.Mutations) {
			t.Errorf("patch for %s was modified on a non-migrated account", target)
		}
	}
}

// Unknown mapping: even on a migrated account, a chat without a PN↔LID
// mapping keeps today's behavior.
func TestExpandPatchForLID_NoMappingUnchanged(t *testing.T) {
	env := newLIDWriteEnv(t, true)
	ctx := context.Background()
	unknownPN := types.JID{User: "51999999999", Server: types.DefaultUserServer}
	patch := BuildArchive(unknownPN, true, time.Now(), nil)
	expanded, err := env.proc.ExpandPatchForLID(ctx, patch)
	if err != nil {
		t.Fatalf("ExpandPatchForLID: %v", err)
	}
	if !reflect.DeepEqual(expanded.Mutations, patch.Mutations) {
		t.Errorf("patch for unmapped chat was modified")
	}
}

// Full encode → decode round trip through whatsmeow's own decoder with MAC
// validation, for a patch with SET+REMOVE mutations under mixed key IDs.
// Verifies: content MACs with the REMOVE operation, per-mutation index keys,
// snapshot MAC (i.e. the LTHash subtracted the removed entries' stored value
// MACs), patch MAC, and the resulting local MAC store. Fails without the
// EncodePatch changes.
func TestEncodeDecodeRoundTripSetRemoveMixedKeys(t *testing.T) {
	env := newLIDWriteEnv(t, true)
	ctx := context.Background()

	patch := BuildArchive(env.lid, true, time.Now(), nil)
	expanded, err := env.proc.ExpandPatchForLID(ctx, patch)
	if err != nil {
		t.Fatalf("ExpandPatchForLID: %v", err)
	}
	encoded, err := env.proc.EncodePatch(ctx, env.newKeyID, env.state, expanded)
	if err != nil {
		t.Fatalf("EncodePatch: %v", err)
	}

	var sp waServerSync.SyncdPatch
	if err = proto.Unmarshal(encoded, &sp); err != nil {
		t.Fatalf("unmarshal patch: %v", err)
	}
	if len(sp.GetMutations()) != 4 {
		t.Fatalf("expected 4 encoded mutations, got %d", len(sp.GetMutations()))
	}
	keyIDs := map[string]int{}
	for _, m := range sp.GetMutations() {
		keyIDs[string(m.GetRecord().GetKeyID().GetID())]++
	}
	if keyIDs[string(env.newKeyID)] != 2 || keyIDs[string(env.oldKeyID)] != 2 {
		t.Fatalf("expected 2 mutations under each key, got %v", keyIDs)
	}
	// The server assigns the version; emulate it for the decode.
	sp.Version = &waServerSync.SyncdVersion{Version: proto.Uint64(env.state.Version + 1)}

	list := &PatchList{Name: WAPatchRegularLow, Patches: []*waServerSync.SyncdPatch{&sp}}
	mutations, newState, err := env.proc.DecodePatches(ctx, list, env.state, true)
	if err != nil {
		t.Fatalf("DecodePatches with validateMACs failed: %v", err)
	}
	if newState.Version != env.state.Version+1 {
		t.Errorf("expected new version %d, got %d", env.state.Version+1, newState.Version)
	}
	var setValueMACs [][]byte
	removeCount := 0
	for _, m := range mutations {
		switch m.Operation {
		case waServerSync.SyncdMutation_SET:
			setValueMACs = append(setValueMACs, m.ValueMAC)
		case waServerSync.SyncdMutation_REMOVE:
			removeCount++
			if m.Index[1] != env.pn.String() {
				t.Errorf("decoded REMOVE index is not the PN: %v", m.Index)
			}
		}
	}
	if removeCount != 2 {
		t.Fatalf("expected 2 decoded REMOVE mutations, got %d", removeCount)
	}
	// All previous entries (archive PN, pin PN, archive LID) were subtracted,
	// so the final hash must equal the sum of just the new SET value MACs.
	expectedState := buildState(newState.Version, setValueMACs)
	if newState.Hash != expectedState.Hash {
		t.Errorf("final LTHash does not match the sum of the new value MACs")
	}

	// The PN-twin rows must be gone from the local MAC store, the LID rows
	// must be stored.
	oldKeyData := env.keyStore.keys[string(env.oldKeyID)].Data
	newKeyData := env.keyStore.keys[string(env.newKeyID)].Data
	for _, index := range [][]string{{IndexArchive, env.pn.String()}, {IndexPin, env.pn.String()}} {
		mac, _ := env.appState.GetAppStateMutationMAC(ctx, string(WAPatchRegularLow), indexMACFor(t, oldKeyData, index))
		if mac != nil {
			t.Errorf("PN twin %v still present in local MAC store", index)
		}
	}
	for _, index := range [][]string{{IndexArchive, env.lid.String()}, {IndexPin, env.lid.String()}} {
		mac, _ := env.appState.GetAppStateMutationMAC(ctx, string(WAPatchRegularLow), indexMACFor(t, newKeyData, index))
		if mac == nil {
			t.Errorf("LID entry %v missing from local MAC store", index)
		} else if string(index[0]) == IndexArchive && string(mac) == string(env.vArchiveLID) {
			t.Errorf("LID archive entry %v was not updated", index)
		}
	}
}

// EncodePatch without the new fields must behave exactly as before: all
// mutations SET under the patch key.
func TestEncodePatchDefaultsUnchanged(t *testing.T) {
	env := newLIDWriteEnv(t, false)
	ctx := context.Background()
	patch := BuildMute(types.JID{User: "51999999999", Server: types.DefaultUserServer}, true, time.Hour)
	encoded, err := env.proc.EncodePatch(ctx, env.newKeyID, HashState{Version: 5}, patch)
	if err != nil {
		t.Fatalf("EncodePatch: %v", err)
	}
	var sp waServerSync.SyncdPatch
	if err = proto.Unmarshal(encoded, &sp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	for _, m := range sp.GetMutations() {
		if m.GetOperation() != waServerSync.SyncdMutation_SET {
			t.Errorf("expected SET operation, got %s", m.GetOperation())
		}
		if string(m.GetRecord().GetKeyID().GetID()) != string(env.newKeyID) {
			t.Errorf("expected patch key, got %x", m.GetRecord().GetKeyID().GetID())
		}
	}
}

// assertNoRepeatedIndexPerOperation mirrors WhatsApp Web's
// validateNoSameIndexForMultipleMutations, which fails a whole patch when a
// plaintext index repeats among its SETs or among its REMOVEs.
func assertNoRepeatedIndexPerOperation(t *testing.T, mutations []MutationInfo) {
	t.Helper()
	if dup := duplicateMutationIndex(mutations); dup != "" {
		for _, m := range mutations {
			t.Logf("mutation: %s", mutationSummary(m))
		}
		t.Fatalf("patch repeats index %s within one operation; WhatsApp Web would reject it as fatal", dup)
	}
}

// A PN twin stored under two different keys is ambiguous: removing it twice
// would repeat its index among the REMOVEs, which WhatsApp Web treats as a
// fatal error, so it must be skipped while the rest of the expansion goes on.
func TestExpandPatchForLID_TwinUnderTwoKeysIsNotRemoved(t *testing.T) {
	env := newLIDWriteEnv(t, true)
	ctx := context.Background()
	newKey, err := env.keyStore.GetAppStateSyncKey(ctx, env.newKeyID)
	if err != nil || newKey == nil {
		t.Fatalf("new key: %v", err)
	}
	// Same plaintext archive index under the PN, now also stored under the new key.
	if err := env.appState.PutAppStateMutationMACs(ctx, string(WAPatchRegularLow), 101, []store.AppStateMutationMAC{{
		IndexMAC: indexMACFor(t, newKey.Data, []string{IndexArchive, env.pn.String()}),
		ValueMAC: randBytes(t, 32),
	}}); err != nil {
		t.Fatal(err)
	}
	expanded, err := env.proc.ExpandPatchForLID(ctx, BuildArchive(env.lid, true, time.Now(), nil))
	if err != nil {
		t.Fatalf("ExpandPatchForLID: %v", err)
	}
	assertNoRepeatedIndexPerOperation(t, expanded.Mutations)
	for _, m := range expanded.Mutations {
		if m.Operation == waServerSync.SyncdMutation_REMOVE && m.Index[0] == IndexArchive {
			t.Fatalf("ambiguous archive twin must not be removed, got %s", mutationSummary(m))
		}
	}
	var pinRemoved bool
	for _, m := range expanded.Mutations {
		if m.Operation == waServerSync.SyncdMutation_REMOVE && m.Index[0] == IndexPin {
			pinRemoved = true
		}
	}
	if !pinRemoved {
		t.Fatal("the unambiguous pin_v1 twin should still be removed")
	}
}

// If rewriting to the LID would make two SETs share an index, the patch must go
// out unexpanded rather than as one WhatsApp Web rejects as fatal.
func TestExpandPatchForLID_RepeatedSetIndexFallsBackUnchanged(t *testing.T) {
	env := newLIDWriteEnv(t, true)
	ctx := context.Background()
	patch := BuildArchive(env.pn, true, time.Now(), nil)
	patch.Mutations = append(patch.Mutations, MutationInfo{
		Index:   []string{IndexArchive, env.lid.String()},
		Version: patch.Mutations[0].Version,
		Value:   patch.Mutations[0].Value,
	})
	expanded, err := env.proc.ExpandPatchForLID(ctx, patch)
	if err != nil {
		t.Fatalf("ExpandPatchForLID: %v", err)
	}
	if len(expanded.Mutations) != len(patch.Mutations) {
		t.Fatalf("expected the original %d mutations, got %d", len(patch.Mutations), len(expanded.Mutations))
	}
	for i := range patch.Mutations {
		if mutationSummary(expanded.Mutations[i]) != mutationSummary(patch.Mutations[i]) {
			t.Fatalf("mutation %d changed: %s != %s", i, mutationSummary(expanded.Mutations[i]), mutationSummary(patch.Mutations[i]))
		}
	}
}

// rotateKey simulates an app state key rotation by the primary: a new key
// becomes the latest, so every previously stored row is now under an old key.
func rotateKey(t *testing.T, env *lidWriteEnv) (keyID, keyData []byte) {
	t.Helper()
	keyID = []byte{0, 0, 0, 0, 0xf3, 0xa7}
	keyData = randBytes(t, 32)
	env.keyStore.addKey(keyID, keyData)
	latest, err := env.keyStore.GetLatestAppStateSyncKeyID(context.Background())
	if err != nil || string(latest) != string(keyID) {
		t.Fatalf("rotated key is not the latest: %x, %v", latest, err)
	}
	return
}

// After a key rotation, a SET of an index whose stored row is under the
// now-old key must be accompanied by a REMOVE of that index under the old
// key, appended after every other mutation (WA Web _generateMutationsToUpload
// $()). Fails without the rotation cleanup in ExpandPatchForLID.
func TestExpandPatchForLID_KeyRotationRemove(t *testing.T) {
	env := newLIDWriteEnv(t, true)
	rotKeyID, _ := rotateKey(t, env)
	ctx := context.Background()

	expanded, err := env.proc.ExpandPatchForLID(ctx, BuildArchive(env.lid, true, time.Now(), nil))
	if err != nil {
		t.Fatalf("ExpandPatchForLID: %v", err)
	}
	// SET archive LID + SET pin LID + REMOVE archive PN + REMOVE pin PN +
	// REMOVE archive LID (stored under env.newKeyID, now one rotation old).
	if len(expanded.Mutations) != 5 {
		for _, m := range expanded.Mutations {
			t.Logf("mutation: %s", mutationSummary(m))
		}
		t.Fatalf("expected 5 mutations, got %d", len(expanded.Mutations))
	}
	assertNoRepeatedIndexPerOperation(t, expanded.Mutations)
	last := expanded.Mutations[len(expanded.Mutations)-1]
	if last.Operation != waServerSync.SyncdMutation_REMOVE ||
		last.Index[0] != IndexArchive || last.Index[1] != env.lid.String() {
		t.Fatalf("last mutation should be the rotation REMOVE of the LID index, got %s", mutationSummary(last))
	}
	if string(last.KeyID) != string(env.newKeyID) {
		t.Errorf("rotation REMOVE should use the stored row's old key %x, got %x", env.newKeyID, last.KeyID)
	}
	if string(last.KeyID) == string(rotKeyID) {
		t.Errorf("rotation REMOVE must not use the latest key")
	}
	// The pin_v1 LID index has no stored row, so it must not get a REMOVE.
	for _, m := range expanded.Mutations {
		if m.Operation == waServerSync.SyncdMutation_REMOVE && m.Index[0] == IndexPin && m.Index[1] == env.lid.String() {
			t.Errorf("pin_v1 LID index has no stored row but got a REMOVE: %s", mutationSummary(m))
		}
	}
}

// An index stored under two or more old keys is ambiguous: removing it twice
// would repeat its index among the REMOVEs (fatal for WhatsApp Web), so it is
// skipped while the rest of the expansion (the PN twins) goes on.
func TestExpandPatchForLID_KeyRotationAmbiguousSkipped(t *testing.T) {
	env := newLIDWriteEnv(t, true)
	ctx := context.Background()
	// The LID archive row exists under env.newKeyID; store it under the
	// (even older) env.oldKeyID too, then rotate so both are old keys.
	oldKey, err := env.keyStore.GetAppStateSyncKey(ctx, env.oldKeyID)
	if err != nil || oldKey == nil {
		t.Fatalf("old key: %v", err)
	}
	if err := env.appState.PutAppStateMutationMACs(ctx, string(WAPatchRegularLow), 101, []store.AppStateMutationMAC{{
		IndexMAC: indexMACFor(t, oldKey.Data, []string{IndexArchive, env.lid.String()}),
		ValueMAC: randBytes(t, 32),
	}}); err != nil {
		t.Fatal(err)
	}
	rotateKey(t, env)

	expanded, err := env.proc.ExpandPatchForLID(ctx, BuildArchive(env.lid, true, time.Now(), nil))
	if err != nil {
		t.Fatalf("ExpandPatchForLID: %v", err)
	}
	assertNoRepeatedIndexPerOperation(t, expanded.Mutations)
	for _, m := range expanded.Mutations {
		if m.Operation == waServerSync.SyncdMutation_REMOVE && m.Index[1] == env.lid.String() {
			t.Fatalf("ambiguous LID index (stored under 2 old keys) must not be removed, got %s", mutationSummary(m))
		}
	}
	// The unambiguous PN twins must still be cleaned up.
	pnRemoves := 0
	for _, m := range expanded.Mutations {
		if m.Operation == waServerSync.SyncdMutation_REMOVE && m.Index[1] == env.pn.String() {
			pnRemoves++
		}
	}
	if pnRemoves != 2 {
		t.Fatalf("expected the 2 PN-twin REMOVEs, got %d", pnRemoves)
	}
}

// waWebRow models one row of WhatsApp Web's sync action store: one record per
// plaintext index, remembering the key it was written under and its value MAC.
type waWebRow struct {
	index    []string
	keyData  []byte
	valueMAC []byte
}

type encodedMutation struct {
	op       waServerSync.SyncdMutation_SyncdOperation
	index    []string // plaintext index (aligned with the expanded patch)
	indexMAC []byte
	valueMAC []byte
}

// waWebLTHashByIndexMAC replays WAWebSyncdAntiTampering Z() in its default
// mode: previous values are looked up by index MAC. SETs with a stored row
// overwrite (subtract) it, REMOVEs with a stored row subtract it, SET value
// MACs are added.
func waWebLTHashByIndexMAC(initial [128]byte, rows []waWebRow, muts []encodedMutation) [128]byte {
	stored := make(map[string][]byte, len(rows))
	for _, row := range rows {
		indexBytes, _ := json.Marshal(row.index)
		mac := concatAndHMAC(sha256.New, expandAppStateKeys(row.keyData).Index, indexBytes)
		stored[string(mac)] = row.valueMAC
	}
	var added, removed [][]byte
	for _, m := range muts {
		if prev, ok := stored[string(m.indexMAC)]; ok {
			removed = append(removed, prev)
		}
		if m.op == waServerSync.SyncdMutation_SET {
			added = append(added, m.valueMAC)
		}
	}
	out := initial
	lthash.WAPatchIntegrity.SubtractThenAddInPlace(out[:], removed, added)
	return out
}

// waWebLTHashByPlaintextIndex replays WAWebSyncdAntiTampering Z() with
// syncd_use_index_for_lthash_lookup: previous values are looked up by
// plaintext index, and a SET does NOT subtract the stored row when the same
// patch also carries a REMOVE of that index (the g-set guard).
func waWebLTHashByPlaintextIndex(initial [128]byte, rows []waWebRow, muts []encodedMutation) [128]byte {
	stored := make(map[string][]byte, len(rows))
	for _, row := range rows {
		indexBytes, _ := json.Marshal(row.index)
		stored[string(indexBytes)] = row.valueMAC
	}
	removesInPatch := make(map[string]bool)
	for _, m := range muts {
		if m.op == waServerSync.SyncdMutation_REMOVE {
			indexBytes, _ := json.Marshal(m.index)
			removesInPatch[string(indexBytes)] = true
		}
	}
	var added, removed [][]byte
	for _, m := range muts {
		indexBytes, _ := json.Marshal(m.index)
		prev, ok := stored[string(indexBytes)]
		if ok {
			if m.op == waServerSync.SyncdMutation_REMOVE {
				removed = append(removed, prev)
			} else if !removesInPatch[string(indexBytes)] {
				removed = append(removed, prev)
			}
		}
		if m.op == waServerSync.SyncdMutation_SET {
			added = append(added, m.valueMAC)
		}
	}
	out := initial
	lthash.WAPatchIntegrity.SubtractThenAddInPlace(out[:], removed, added)
	return out
}

// encodeAndExtract encodes the patch under patchKeyID and returns the encoded
// mutations (aligned with the patch's plaintext indexes), the sender-side
// state after updateHash (what EncodePatch signs into the snapshot MAC), and
// the raw SyncdPatch. Must be called before anything mutates env.appState.
func encodeAndExtract(t *testing.T, env *lidWriteEnv, patchKeyID []byte, patch PatchInfo) ([]encodedMutation, HashState, *waServerSync.SyncdPatch) {
	t.Helper()
	ctx := context.Background()
	encoded, err := env.proc.EncodePatch(ctx, patchKeyID, env.state, patch)
	if err != nil {
		t.Fatalf("EncodePatch: %v", err)
	}
	var sp waServerSync.SyncdPatch
	if err = proto.Unmarshal(encoded, &sp); err != nil {
		t.Fatalf("unmarshal patch: %v", err)
	}
	if len(sp.GetMutations()) != len(patch.Mutations) {
		t.Fatalf("encoded %d mutations, patch has %d", len(sp.GetMutations()), len(patch.Mutations))
	}
	out := make([]encodedMutation, len(sp.GetMutations()))
	for i, m := range sp.GetMutations() {
		value := m.GetRecord().GetValue().GetBlob()
		out[i] = encodedMutation{
			op:       m.GetOperation(),
			index:    patch.Mutations[i].Index,
			indexMAC: m.GetRecord().GetIndex().GetBlob(),
			valueMAC: value[len(value)-32:],
		}
	}
	senderState := env.state
	warn, err := senderState.updateHash(sp.GetMutations(), func(indexMAC []byte, _ int) ([]byte, error) {
		return env.appState.GetAppStateMutationMAC(ctx, string(patch.Type), indexMAC)
	})
	if err != nil {
		t.Fatalf("updateHash: %v", err)
	}
	if len(warn) > 0 {
		t.Logf("updateHash warnings: %v", warn)
	}
	senderState.Version++
	return out, senderState, &sp
}

// waWebRowsFor builds the WhatsApp Web receiver's view of the seeded state:
// one row per plaintext index, each under the key it was written with.
func waWebRowsFor(env *lidWriteEnv) []waWebRow {
	oldKeyData := env.keyStore.keys[string(env.oldKeyID)].Data
	lidKeyData := env.keyStore.keys[string(env.newKeyID)].Data
	return []waWebRow{
		{index: []string{IndexArchive, env.pn.String()}, keyData: oldKeyData, valueMAC: env.vArchivePN},
		{index: []string{IndexPin, env.pn.String()}, keyData: oldKeyData, valueMAC: env.vPinPN},
		{index: []string{IndexArchive, env.lid.String()}, keyData: lidKeyData, valueMAC: env.vArchiveLID},
	}
}

// The LTHash whatsmeow computes when sending an expanded patch must equal
// what a WhatsApp Web receiver computes applying it, in BOTH of WA Web's
// lookup modes (by index MAC and by plaintext index, WAWebSyncdAntiTampering
// Z()). Covers the plain migrated account and the key-rotation case; the
// rotation subtests fail without the rotation REMOVE in ExpandPatchForLID.
func TestLIDWriteLTHashMatchesWAWebBothModes(t *testing.T) {
	scenarios := []struct {
		name   string
		rotate bool
	}{
		{"SameKey", false},
		{"Rotated", true},
	}
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			env := newLIDWriteEnv(t, true)
			ctx := context.Background()
			patchKeyID := env.newKeyID
			if sc.rotate {
				patchKeyID, _ = rotateKey(t, env)
			}
			rows := waWebRowsFor(env)

			expanded, err := env.proc.ExpandPatchForLID(ctx, BuildArchive(env.lid, true, time.Now(), nil))
			if err != nil {
				t.Fatalf("ExpandPatchForLID: %v", err)
			}
			muts, senderState, sp := encodeAndExtract(t, env, patchKeyID, expanded)

			// Cross-check the sender hash through the real decoder with MAC
			// validation (also what a whatsmeow receiver would store).
			sp.Version = &waServerSync.SyncdVersion{Version: proto.Uint64(env.state.Version + 1)}
			list := &PatchList{Name: WAPatchRegularLow, Patches: []*waServerSync.SyncdPatch{sp}}
			_, newState, err := env.proc.DecodePatches(ctx, list, env.state, true)
			if err != nil {
				t.Fatalf("DecodePatches: %v", err)
			}
			if newState.Hash != senderState.Hash {
				t.Fatalf("decoder state and sender state disagree")
			}

			byMAC := waWebLTHashByIndexMAC(env.state.Hash, rows, muts)
			byIndex := waWebLTHashByPlaintextIndex(env.state.Hash, rows, muts)
			if byMAC != senderState.Hash {
				t.Errorf("WA Web by-index-MAC hash differs from whatsmeow's")
			}
			if byIndex != senderState.Hash {
				t.Errorf("WA Web by-plaintext-index hash differs from whatsmeow's")
			}
		})
	}
}

// Sanity check that the equivalence above is not vacuous: encoding the same
// rotated patch WITHOUT the rotation REMOVE (what the fork did before this
// fix) makes the plaintext-index mode diverge from whatsmeow's hash.
func TestLIDWriteLTHashDivergesWithoutRotationRemove(t *testing.T) {
	env := newLIDWriteEnv(t, true)
	ctx := context.Background()
	patchKeyID, _ := rotateKey(t, env)
	rows := waWebRowsFor(env)

	expanded, err := env.proc.ExpandPatchForLID(ctx, BuildArchive(env.lid, true, time.Now(), nil))
	if err != nil {
		t.Fatalf("ExpandPatchForLID: %v", err)
	}
	stripped := expanded
	stripped.Mutations = nil
	for _, m := range expanded.Mutations {
		if m.Operation == waServerSync.SyncdMutation_REMOVE && m.Index[1] == env.lid.String() {
			continue
		}
		stripped.Mutations = append(stripped.Mutations, m)
	}
	if len(stripped.Mutations) != len(expanded.Mutations)-1 {
		t.Fatalf("expected to strip exactly the rotation REMOVE (have %d, want %d)",
			len(stripped.Mutations), len(expanded.Mutations)-1)
	}
	muts, senderState, _ := encodeAndExtract(t, env, patchKeyID, stripped)
	if byMAC := waWebLTHashByIndexMAC(env.state.Hash, rows, muts); byMAC == senderState.Hash {
		t.Logf("by-index-MAC mode matches even without the REMOVE (expected: whatsmeow and that mode subtract the same rows)")
	}
	if byIndex := waWebLTHashByPlaintextIndex(env.state.Hash, rows, muts); byIndex == senderState.Hash {
		t.Errorf("plaintext-index mode should diverge without the rotation REMOVE; the equivalence test is vacuous")
	}
}

// Every chat action the fork rewrites must keep one mutation per index and
// operation, archived or not, PN or LID target.
func TestExpandPatchForLID_NoRepeatedIndexes(t *testing.T) {
	env := newLIDWriteEnv(t, true)
	ctx := context.Background()
	for _, target := range []types.JID{env.pn, env.lid} {
		for _, patch := range []PatchInfo{
			BuildArchive(target, true, time.Now(), nil),
			BuildArchive(target, false, time.Now(), nil),
			BuildPin(target, true),
			BuildMarkChatAsRead(target, true, time.Now(), nil),
		} {
			expanded, err := env.proc.ExpandPatchForLID(ctx, patch)
			if err != nil {
				t.Fatalf("ExpandPatchForLID(%v): %v", patch.Mutations[0].Index, err)
			}
			assertNoRepeatedIndexPerOperation(t, expanded.Mutations)
		}
	}
}
