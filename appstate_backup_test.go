package whatsmeow

import (
	"context"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
)

// fixcheckSessionDB is a copy (made with SQLite's online backup) of a real
// production session.db whose primary migrated chats to LID, so its
// regular_low collection (v1650) can only come from a phone recovery. The
// test is skipped when the copy is not present.
const fixcheckSessionDB = "/tmp/opencode/syncd-research/data/fixcheck/session.db"

func TestCaptureAndRestoreAppStateVersion(t *testing.T) {
	if _, err := os.Stat(fixcheckSessionDB); err != nil {
		t.Skipf("real session.db copy not available: %v", err)
	}
	ctx := context.Background()
	// Work on a throwaway copy: the test deletes and restores state.
	dbPath := filepath.Join(t.TempDir(), "session.db")
	src, err := os.Open(fixcheckSessionDB)
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	dst, err := os.Create(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = io.Copy(dst, src); err != nil {
		t.Fatal(err)
	}
	if err = dst.Close(); err != nil {
		t.Fatal(err)
	}

	// Same connection parameters as wacli's production session store.
	container, err := sqlstore.New(ctx, "sqlite3", "file:"+dbPath+"?_foreign_keys=on", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer container.Close()
	device, err := container.GetFirstDevice(ctx)
	if err != nil {
		t.Fatal(err)
	}
	appStateStore := device.AppState

	for _, name := range []string{"regular_low", "regular_high", "regular"} {
		t.Run(name, func(t *testing.T) {
			origVersion, origHash, err := appStateStore.GetAppStateVersion(ctx, name)
			if err != nil {
				t.Fatal(err)
			}
			if origVersion == 0 {
				t.Fatalf("collection %s has no stored version in the real database", name)
			}
			origMACs := dumpLatestMACs(ctx, t, appStateStore, name)
			if len(origMACs) == 0 {
				t.Fatalf("collection %s has no stored mutation MACs in the real database", name)
			}

			backup, err := captureAppStateVersion(ctx, appStateStore, name)
			if err != nil {
				t.Fatalf("capture failed: %v", err)
			}
			if backup == nil {
				t.Fatal("capture returned nil for a non-empty collection")
			}
			if backup.Version != origVersion || backup.Hash != origHash {
				t.Fatalf("captured version/hash = %d/%x, want %d/%x", backup.Version, backup.Hash[:8], origVersion, origHash[:8])
			}

			// Simulate what fetchAppState does on a full sync.
			if err = appStateStore.DeleteAppStateVersion(ctx, name); err != nil {
				t.Fatal(err)
			}
			if v, _, err := appStateStore.GetAppStateVersion(ctx, name); err != nil || v != 0 {
				t.Fatalf("after delete: version=%d err=%v, want 0/nil", v, err)
			}
			if leftover := dumpLatestMACs(ctx, t, appStateStore, name); len(leftover) != 0 {
				t.Fatalf("after delete: %d mutation MACs survived (foreign keys off?)", len(leftover))
			}

			if err = restoreAppStateVersion(ctx, appStateStore, name, backup); err != nil {
				t.Fatalf("restore failed: %v", err)
			}

			gotVersion, gotHash, err := appStateStore.GetAppStateVersion(ctx, name)
			if err != nil {
				t.Fatal(err)
			}
			if gotVersion != origVersion {
				t.Fatalf("restored version = %d, want %d", gotVersion, origVersion)
			}
			if gotHash != origHash {
				t.Fatalf("restored hash = %x, want %x", gotHash[:8], origHash[:8])
			}
			gotMACs := dumpLatestMACs(ctx, t, appStateStore, name)
			if len(gotMACs) != len(origMACs) {
				t.Fatalf("restored %d index MACs, want %d", len(gotMACs), len(origMACs))
			}
			for indexMAC, valueMAC := range origMACs {
				if gotMACs[indexMAC] != valueMAC {
					t.Fatalf("index MAC %s: restored value %s, want %s", indexMAC, gotMACs[indexMAC], valueMAC)
				}
			}
			// GetAppStateMutationMAC (the lookup DecodePatches uses) must see
			// the same values too.
			checked := 0
			for indexMAC, valueMAC := range origMACs {
				raw, err := hex.DecodeString(indexMAC)
				if err != nil {
					t.Fatal(err)
				}
				got, err := appStateStore.GetAppStateMutationMAC(ctx, name, raw)
				if err != nil {
					t.Fatal(err)
				}
				if hex.EncodeToString(got) != valueMAC {
					t.Fatalf("GetAppStateMutationMAC(%s) = %x, want %s", indexMAC, got, valueMAC)
				}
				checked++
				if checked >= 25 {
					break
				}
			}
		})
	}
}

// fakeAppStateVersionStore is a minimal in-memory store.AppStateStore (plus
// the MAC dumper extension) for exercising the capture/restore helpers.
type fakeAppStateVersionStore struct {
	version uint64
	hash    [128]byte
	macs    map[string][]byte // index MAC → value MAC (latest only)
}

func newFakeAppStateVersionStore() *fakeAppStateVersionStore {
	return &fakeAppStateVersionStore{macs: make(map[string][]byte)}
}

func (f *fakeAppStateVersionStore) PutAppStateVersion(ctx context.Context, name string, version uint64, hash [128]byte) error {
	f.version, f.hash = version, hash
	return nil
}

func (f *fakeAppStateVersionStore) GetAppStateVersion(ctx context.Context, name string) (uint64, [128]byte, error) {
	return f.version, f.hash, nil
}

func (f *fakeAppStateVersionStore) DeleteAppStateVersion(ctx context.Context, name string) error {
	f.version, f.hash = 0, [128]byte{}
	f.macs = make(map[string][]byte) // the version row cascades to its MACs
	return nil
}

func (f *fakeAppStateVersionStore) PutAppStateMutationMACs(ctx context.Context, name string, version uint64, mutations []store.AppStateMutationMAC) error {
	for _, m := range mutations {
		f.macs[string(m.IndexMAC)] = m.ValueMAC
	}
	return nil
}

func (f *fakeAppStateVersionStore) DeleteAppStateMutationMACs(ctx context.Context, name string, indexMACs [][]byte) error {
	for _, mac := range indexMACs {
		delete(f.macs, string(mac))
	}
	return nil
}

func (f *fakeAppStateVersionStore) GetAppStateMutationMAC(ctx context.Context, name string, indexMAC []byte) ([]byte, error) {
	return f.macs[string(indexMAC)], nil
}

func (f *fakeAppStateVersionStore) GetAllAppStateMutationMACs(ctx context.Context, name string) ([]store.AppStateMutationMAC, error) {
	out := make([]store.AppStateMutationMAC, 0, len(f.macs))
	for idx, val := range f.macs {
		out = append(out, store.AppStateMutationMAC{IndexMAC: []byte(idx), ValueMAC: val})
	}
	return out, nil
}

// A failed full sync must NOT restore its pre-sync capture over a state that a
// recovery response adopted (at an equal or newer version) while the sync was
// running; it must still restore when nothing newer was written. This is the
// store-level half of the fetchAppState/handleAppStateRecovery race fix (the
// other half is that both sections run under appStateRecoveryLock). Fails
// without restoreAppStateVersionIfOlder.
func TestRestoreSkippedWhenRecoveryAdoptedMidSync(t *testing.T) {
	ctx := context.Background()
	mac := func(b byte) []byte {
		out := make([]byte, 32)
		for i := range out {
			out[i] = b
		}
		return out
	}
	seed := func(f *fakeAppStateVersionStore, version uint64, hashByte byte, macByte byte) {
		var hash [128]byte
		hash[0] = hashByte
		_ = f.PutAppStateVersion(ctx, "regular_low", version, hash)
		_ = f.PutAppStateMutationMACs(ctx, "regular_low", version, []store.AppStateMutationMAC{{IndexMAC: mac(macByte), ValueMAC: mac(macByte + 1)}})
	}

	for _, tc := range []struct {
		name            string
		recoveryVersion uint64 // 0 = no recovery arrives mid-sync
		wantRestored    bool
		wantVersion     uint64
	}{
		{"NoRecovery", 0, true, 1650},
		{"RecoverySameVersion", 1650, false, 1650},
		{"RecoveryNewerVersion", 1651, false, 1651},
		{"RecoveryOlderVersion", 1649, true, 1650},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeAppStateVersionStore()
			seed(f, 1650, 0x01, 0x10)

			// fetchAppState: capture + delete (under appStateRecoveryLock).
			backup, err := captureAppStateVersion(ctx, f, "regular_low")
			if err != nil || backup == nil {
				t.Fatalf("capture: %v (backup=%v)", err, backup)
			}
			if err = f.DeleteAppStateVersion(ctx, "regular_low"); err != nil {
				t.Fatal(err)
			}
			// Recovery response adopted mid-sync (ProcessRecovery writes,
			// serialized by the same lock in handleAppStateRecovery).
			if tc.recoveryVersion != 0 {
				seed(f, tc.recoveryVersion, 0x02, 0x20)
			}
			// The full sync fails and tries to restore.
			restored, err := restoreAppStateVersionIfOlder(ctx, f, "regular_low", backup)
			if err != nil {
				t.Fatalf("restore: %v", err)
			}
			if restored != tc.wantRestored {
				t.Fatalf("restored = %v, want %v", restored, tc.wantRestored)
			}
			version, hash, err := f.GetAppStateVersion(ctx, "regular_low")
			if err != nil {
				t.Fatal(err)
			}
			if version != tc.wantVersion {
				t.Fatalf("final version = %d, want %d", version, tc.wantVersion)
			}
			if tc.wantRestored {
				if hash != backup.Hash {
					t.Errorf("restored hash does not match the capture")
				}
				if got, _ := f.GetAppStateMutationMAC(ctx, "regular_low", mac(0x10)); got == nil {
					t.Errorf("restored MACs missing")
				}
			} else {
				// The adopted state must be untouched.
				if hash[0] != 0x02 {
					t.Errorf("adopted hash was overwritten")
				}
				if got, _ := f.GetAppStateMutationMAC(ctx, "regular_low", mac(0x20)); got == nil {
					t.Errorf("adopted MACs were overwritten")
				}
			}
		})
	}
}

func dumpLatestMACs(ctx context.Context, t *testing.T, s store.AppStateStore, name string) map[string]string {
	t.Helper()
	dumper, ok := s.(appStateMutationMACDumper)
	if !ok {
		t.Fatalf("store %T cannot enumerate mutation MACs", s)
	}
	macs, err := dumper.GetAllAppStateMutationMACs(ctx, name)
	if err != nil {
		t.Fatal(err)
	}
	out := make(map[string]string, len(macs))
	for _, mac := range macs {
		out[hex.EncodeToString(mac.IndexMAC)] = hex.EncodeToString(mac.ValueMAC)
	}
	return out
}
