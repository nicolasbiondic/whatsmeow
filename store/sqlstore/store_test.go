package sqlstore

import (
	"bytes"
	"context"
	"encoding/hex"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"go.mau.fi/whatsmeow/proto/waAdv"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
)

func newTestAppStateStore(t *testing.T) *SQLStore {
	t.Helper()
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "session.db")
	container, err := New(ctx, "sqlite3", "file:"+dbPath+"?_foreign_keys=on", nil)
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
	s, ok := device.AppState.(*SQLStore)
	if !ok {
		t.Fatalf("device.AppState is %T, want *SQLStore", device.AppState)
	}
	return s
}

func mac(b byte) []byte {
	out := make([]byte, 32)
	for i := range out {
		out[i] = b
	}
	return out
}

// GetAllAppStateMutationMACs must return exactly one row per index MAC: the
// value stored at its highest version, i.e. the same value that
// GetAppStateMutationMAC (ORDER BY version DESC LIMIT 1) returns. This is the
// latest-per-index semantics that captureAppStateVersion, ExpandPatchForLID
// and the LTHash recovery all rely on, exercised with index MACs that are
// stored at several versions ("duplicate versions").
func TestGetAllAppStateMutationMACsLatestPerIndex(t *testing.T) {
	ctx := context.Background()
	s := newTestAppStateStore(t)
	const name = "regular_low"

	if err := s.PutAppStateVersion(ctx, name, 3, [128]byte{}); err != nil {
		t.Fatalf("put version: %v", err)
	}
	// v1: A→a1, B→b1; v2: B→b2, C→c2; v3: C→c3, D→d3.
	// B and C exist at two versions each; only b2, c3 may be returned.
	put := func(version uint64, macs ...store.AppStateMutationMAC) {
		t.Helper()
		if err := s.PutAppStateMutationMACs(ctx, name, version, macs); err != nil {
			t.Fatalf("put MACs v%d: %v", version, err)
		}
	}
	idxA, idxB, idxC, idxD := mac(0xA0), mac(0xB0), mac(0xC0), mac(0xD0)
	put(1,
		store.AppStateMutationMAC{IndexMAC: idxA, ValueMAC: mac(0xA1)},
		store.AppStateMutationMAC{IndexMAC: idxB, ValueMAC: mac(0xB1)},
	)
	put(2,
		store.AppStateMutationMAC{IndexMAC: idxB, ValueMAC: mac(0xB2)},
		store.AppStateMutationMAC{IndexMAC: idxC, ValueMAC: mac(0xC2)},
	)
	put(3,
		store.AppStateMutationMAC{IndexMAC: idxC, ValueMAC: mac(0xC3)},
		store.AppStateMutationMAC{IndexMAC: idxD, ValueMAC: mac(0xD3)},
	)

	all, err := s.GetAllAppStateMutationMACs(ctx, name)
	if err != nil {
		t.Fatalf("get all: %v", err)
	}
	want := map[string]string{
		hex.EncodeToString(idxA): hex.EncodeToString(mac(0xA1)),
		hex.EncodeToString(idxB): hex.EncodeToString(mac(0xB2)),
		hex.EncodeToString(idxC): hex.EncodeToString(mac(0xC3)),
		hex.EncodeToString(idxD): hex.EncodeToString(mac(0xD3)),
	}
	if len(all) != len(want) {
		t.Fatalf("got %d rows, want %d (one per distinct index MAC): %v", len(all), len(want), all)
	}
	got := make(map[string]string, len(all))
	for _, m := range all {
		idx := hex.EncodeToString(m.IndexMAC)
		if _, dup := got[idx]; dup {
			t.Fatalf("index MAC %s returned more than once", idx)
		}
		got[idx] = hex.EncodeToString(m.ValueMAC)
	}
	for idx, val := range want {
		if got[idx] != val {
			t.Errorf("index %s: got value %s, want %s", idx, got[idx], val)
		}
		// Must match the single-row lookup that DecodePatches/EncodePatch use.
		raw, _ := hex.DecodeString(idx)
		single, err := s.GetAppStateMutationMAC(ctx, name, raw)
		if err != nil {
			t.Fatalf("single lookup: %v", err)
		}
		if hex.EncodeToString(single) != val {
			t.Errorf("index %s: GetAppStateMutationMAC = %x, want %s", idx, single, val)
		}
	}

	// Unknown collection: no rows, no error.
	empty, err := s.GetAllAppStateMutationMACs(ctx, "regular_high")
	if err != nil || len(empty) != 0 {
		t.Fatalf("empty collection: got %d rows, err %v", len(empty), err)
	}
}

// The listing runs while session.db is held (before every full sync and per
// chat write). The first version was a correlated MAX(version) subquery: 74 s
// on a real 34,585-row collection, which locked session.db long enough to drop
// incoming message decryption. Guard the set-based query's cost.
func TestGetAllAppStateMutationMACsScalesLinearly(t *testing.T) {
	ctx := context.Background()
	s := newTestAppStateStore(t)
	const name = "regular"
	const rows = 20000
	if err := s.PutAppStateVersion(ctx, name, 2, [128]byte{}); err != nil {
		t.Fatalf("put version: %v", err)
	}
	batch := make([]store.AppStateMutationMAC, 0, rows)
	for i := 0; i < rows; i++ {
		index := make([]byte, 32)
		index[0], index[1], index[2] = byte(i>>16), byte(i>>8), byte(i)
		batch = append(batch, store.AppStateMutationMAC{IndexMAC: index, ValueMAC: mac(byte(i))})
	}
	if err := s.PutAppStateMutationMACs(ctx, name, 1, batch); err != nil {
		t.Fatalf("put MACs: %v", err)
	}
	start := time.Now()
	got, err := s.GetAllAppStateMutationMACs(ctx, name)
	if err != nil {
		t.Fatalf("GetAllAppStateMutationMACs: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("listing %d rows took %v; it must stay well under the session lock budget", rows, elapsed)
	}
	if len(got) != rows {
		t.Fatalf("got %d rows, want %d", len(got), rows)
	}
}

// PutAppStatePatch must persist the version/hash row, the deletes and the
// inserts of one patch all or nothing. Before it existed, DecodePatches wrote
// them as three separate statements, so a UNIQUE violation on the insert left
// the new LTHash stored with the old MACs (and any deletes already applied).
func TestPutAppStatePatchIsAtomic(t *testing.T) {
	ctx := context.Background()
	s := newTestAppStateStore(t)
	const name = "regular_low"
	var _ store.AppStatePatchStore = s

	idxA, idxB := mac(0xA0), mac(0xB0)
	var h1, h2, h3 [128]byte
	h1[0], h2[0], h3[0] = 1, 2, 3

	// v1: A→a1, B→b1.
	if err := s.PutAppStatePatch(ctx, name, 1, h1, nil, []store.AppStateMutationMAC{
		{IndexMAC: idxA, ValueMAC: mac(0xA1)},
		{IndexMAC: idxB, ValueMAC: mac(0xB1)},
	}); err != nil {
		t.Fatalf("put v1: %v", err)
	}
	// v2: A→a2, B removed.
	if err := s.PutAppStatePatch(ctx, name, 2, h2, [][]byte{idxB}, []store.AppStateMutationMAC{
		{IndexMAC: idxA, ValueMAC: mac(0xA2)},
	}); err != nil {
		t.Fatalf("put v2: %v", err)
	}
	check := func(wantVersion uint64, wantHash [128]byte, wantA, wantB []byte) {
		t.Helper()
		version, hash, err := s.GetAppStateVersion(ctx, name)
		if err != nil {
			t.Fatalf("get version: %v", err)
		}
		if version != wantVersion || hash != wantHash {
			t.Errorf("stored version/hash = %d/%x, want %d/%x", version, hash[:1], wantVersion, wantHash[:1])
		}
		gotA, err := s.GetAppStateMutationMAC(ctx, name, idxA)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(gotA, wantA) {
			t.Errorf("A = %x, want %x", gotA, wantA)
		}
		gotB, err := s.GetAppStateMutationMAC(ctx, name, idxB)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(gotB, wantB) {
			t.Errorf("B = %x, want %x", gotB, wantB)
		}
	}
	check(2, h2, mac(0xA2), nil)

	// A second application of "v2" (what the 409 race did): the insert of
	// (v2, A) violates the primary key. Nothing of the call may persist, in
	// particular not the version/hash row written before the insert, nor the
	// delete of A that comes with it.
	err := s.PutAppStatePatch(ctx, name, 2, h3, [][]byte{idxA}, []store.AppStateMutationMAC{
		{IndexMAC: idxA, ValueMAC: mac(0xA3)},
		{IndexMAC: idxA, ValueMAC: mac(0xA2)},
	})
	if err == nil {
		t.Fatal("duplicate (version, index_mac) insert unexpectedly succeeded")
	}
	check(2, h2, mac(0xA2), nil)

	// Same with a version bump: the UNIQUE violation is on an older version's
	// row, so the version/hash update must be rolled back too.
	if err = s.PutAppStateMutationMACs(ctx, name, 3, []store.AppStateMutationMAC{{IndexMAC: idxB, ValueMAC: mac(0xB3)}}); err != nil {
		t.Fatal(err)
	}
	err = s.PutAppStatePatch(ctx, name, 3, h3, nil, []store.AppStateMutationMAC{{IndexMAC: idxB, ValueMAC: mac(0xB4)}})
	if err == nil {
		t.Fatal("duplicate (version, index_mac) insert unexpectedly succeeded")
	}
	check(2, h2, mac(0xA2), mac(0xB3))
}
