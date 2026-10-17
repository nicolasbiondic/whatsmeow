package appstate

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"google.golang.org/protobuf/proto"

	"go.mau.fi/whatsmeow/proto/waAdv"
	"go.mau.fi/whatsmeow/proto/waServerSync"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// newSQLiteDecodeDevice opens a fresh SQLite session store (the production
// store type) and seeds one app state sync key.
func newSQLiteDecodeDevice(t *testing.T) (*store.Device, []byte) {
	t.Helper()
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "session.db")
	container, err := sqlstore.New(ctx, "sqlite3", "file:"+dbPath+"?_foreign_keys=on", nil)
	if err != nil {
		t.Fatalf("open container: %v", err)
	}
	t.Cleanup(func() { container.Close() })
	device := container.NewDevice()
	device.ID = &types.JID{User: "15551234567", Device: 1, Server: types.DefaultUserServer}
	device.Account = &waAdv.ADVSignedDeviceIdentity{
		Details:             []byte{},
		AccountSignature:    make([]byte, 64),
		AccountSignatureKey: make([]byte, 32),
		DeviceSignature:     make([]byte, 64),
	}
	if err = container.PutDevice(ctx, device); err != nil {
		t.Fatalf("put device: %v", err)
	}
	keyID := []byte{0, 0, 0, 0, 0xf3, 0xa6}
	err = device.AppStateKeys.PutAppStateSyncKey(ctx, keyID, store.AppStateSyncKey{
		Data:        randBytes(t, 32),
		Fingerprint: []byte{1, 2, 3},
		Timestamp:   time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("put app state key: %v", err)
	}
	return device, keyID
}

// encodeArchivePatch encodes "archive chat at time ts" against state and
// stamps it with the given server version, like the server would.
func encodeArchivePatch(t *testing.T, proc *Processor, keyID []byte, state HashState, chat types.JID, ts time.Time, version uint64) *waServerSync.SyncdPatch {
	t.Helper()
	raw, err := proc.EncodePatch(context.Background(), keyID, state, BuildArchive(chat, true, ts, nil))
	if err != nil {
		t.Fatalf("encode patch: %v", err)
	}
	var patch waServerSync.SyncdPatch
	if err = proto.Unmarshal(raw, &patch); err != nil {
		t.Fatal(err)
	}
	patch.Version = &waServerSync.SyncdVersion{Version: proto.Uint64(version)}
	return &patch
}

func valueMACOf(m *waServerSync.SyncdMutation) []byte {
	blob := m.GetRecord().GetValue().GetBlob()
	return blob[len(blob)-32:]
}

// DecodePatches must persist a patch through the store's transactional path
// when the store offers one: if the MAC insert of a verified patch fails, the
// version/hash row written in the same call must not survive. This drives the
// real processor against the real SQLStore and injects the failure by
// pre-inserting a row with the (version, index_mac) primary key the patch is
// about to insert, so it fails if storeMACs falls back to the three separate
// statements (version, delete, insert) even though SQLStore implements
// store.AppStatePatchStore.
func TestDecodePatchesStoresPatchAtomically(t *testing.T) {
	ctx := context.Background()
	device, keyID := newSQLiteDecodeDevice(t)
	if _, ok := device.AppState.(store.AppStatePatchStore); !ok {
		t.Fatalf("%T does not implement store.AppStatePatchStore", device.AppState)
	}
	proc := NewProcessor(device, waLog.Noop)
	const name = WAPatchRegularLow
	chat := types.JID{User: "100000000000001", Server: types.HiddenUserServer}

	// v1: the chat is archived once. Applied through DecodePatches with
	// verification on, like a normal fetch.
	var state0 HashState
	patch1 := encodeArchivePatch(t, proc, keyID, state0, chat, time.Now().Add(-time.Hour), 1)
	_, state1, err := proc.DecodePatches(ctx, &PatchList{Name: name, Patches: []*waServerSync.SyncdPatch{patch1}}, state0, true)
	if err != nil {
		t.Fatalf("decode v1: %v", err)
	}
	indexMAC := patch1.GetMutations()[0].GetRecord().GetIndex().GetBlob()
	value1 := valueMACOf(patch1.GetMutations()[0])
	version, hash, err := device.AppState.GetAppStateVersion(ctx, string(name))
	if err != nil {
		t.Fatal(err)
	}
	if version != 1 || hash != state1.Hash {
		t.Fatalf("stored version/hash after v1 = %d/%x…, want 1/%x…", version, hash[:4], state1.Hash[:4])
	}

	// v2: the same chat archived again (a SET replacing value1), encoded
	// against v1 exactly like another device would.
	patch2 := encodeArchivePatch(t, proc, keyID, state1, chat, time.Now(), 2)

	// Inject the failure: a row that already occupies (v2, indexMAC). Its
	// value is value1, so the processor's "previous value" lookup still
	// returns value1 and the patch verifies; only the final insert collides.
	err = device.AppState.PutAppStateMutationMACs(ctx, string(name), 2, []store.AppStateMutationMAC{{IndexMAC: indexMAC, ValueMAC: value1}})
	if err != nil {
		t.Fatalf("inject conflicting row: %v", err)
	}

	_, _, err = proc.DecodePatches(ctx, &PatchList{Name: name, Patches: []*waServerSync.SyncdPatch{patch2}}, state1, true)
	if err == nil {
		t.Fatal("decoding v2 over a conflicting MAC row unexpectedly succeeded")
	}
	if !strings.Contains(err.Error(), "UNIQUE") {
		t.Fatalf("decode v2 failed for an unexpected reason: %v", err)
	}
	if !strings.Contains(err.Error(), "failed to store app state patch") {
		t.Errorf("decode v2 did not go through the transactional store path: %v", err)
	}

	// Nothing of the failed patch may be visible: version and hash are still
	// v1's and the chat's latest value MAC is still value1.
	version, hash, err = device.AppState.GetAppStateVersion(ctx, string(name))
	if err != nil {
		t.Fatal(err)
	}
	if version != 1 || hash != state1.Hash {
		t.Errorf("failed patch left version/hash = %d/%x…, want 1/%x… (version/hash persisted without its MACs)", version, hash[:4], state1.Hash[:4])
	}
	got, err := device.AppState.GetAppStateMutationMAC(ctx, string(name), indexMAC)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(value1) {
		t.Errorf("latest value MAC = %x…, want value1 %x…", got[:4], value1[:4])
	}
}

// failingMACStore is a plain store.AppStateStore (no AppStatePatchStore) whose
// MAC insert fails, standing in for a third-party store.
type failingMACStore struct {
	*memAppStateStore
	err error
}

func (f *failingMACStore) PutAppStateMutationMACs(context.Context, string, uint64, []store.AppStateMutationMAC) error {
	return f.err
}

// Stores without the optional AppStatePatchStore keep the previous behaviour:
// DecodePatches still reports an insert failure as an error. (That path is
// not atomic; see store.AppStatePatchStore.)
func TestDecodePatchesFallbackStoreReportsInsertError(t *testing.T) {
	ctx := context.Background()
	appState := &failingMACStore{memAppStateStore: newMemAppStateStore(), err: errors.New("injected insert failure")}
	var _ store.AppStateStore = appState
	if _, ok := any(appState).(store.AppStatePatchStore); ok {
		t.Fatal("failingMACStore must not implement store.AppStatePatchStore")
	}
	keyStore := newFakeKeyStore()
	keyID := []byte{0, 0, 0, 0, 0xf3, 0xa6}
	keyStore.addKey(keyID, randBytes(t, 32))
	proc := NewProcessor(&store.Device{AppState: appState, AppStateKeys: keyStore}, waLog.Noop)

	var state0 HashState
	patch := encodeArchivePatch(t, proc, keyID, state0, types.JID{User: "100000000000001", Server: types.HiddenUserServer}, time.Now(), 1)
	_, _, err := proc.DecodePatches(ctx, &PatchList{Name: WAPatchRegularLow, Patches: []*waServerSync.SyncdPatch{patch}}, state0, true)
	if err == nil || !errors.Is(err, appState.err) {
		t.Fatalf("DecodePatches error = %v, want the injected insert failure", err)
	}
	if !strings.Contains(err.Error(), "failed to insert added mutation MACs") {
		t.Errorf("error did not come from the fallback insert step: %v", err)
	}
}
