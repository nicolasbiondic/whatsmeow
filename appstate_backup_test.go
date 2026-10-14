// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"context"
	"testing"

	"go.mau.fi/whatsmeow/store"
)

// fakeAppStateVersionStore is a minimal in-memory store.AppStateStore for
// exercising the capture/restore helpers.
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

func testBackupMAC(b byte) []byte {
	out := make([]byte, 32)
	for i := range out {
		out[i] = b
	}
	return out
}

// Capturing an empty collection must return nil without an error, so a full
// sync of a never-synced collection behaves exactly as before.
func TestCaptureAppStateVersionEmpty(t *testing.T) {
	backup, err := captureAppStateVersion(context.Background(), newFakeAppStateVersionStore(), "regular_low")
	if err != nil {
		t.Fatalf("capture: %v", err)
	}
	if backup != nil {
		t.Fatalf("capture of an empty collection = %+v, want nil", backup)
	}
}

// A failed full sync must restore the captured version, hash and mutation MACs
// exactly, including over partial state the failed sync left behind.
func TestCaptureAndRestoreAppStateVersion(t *testing.T) {
	ctx := context.Background()
	f := newFakeAppStateVersionStore()
	var hash [128]byte
	hash[0], hash[127] = 0x12, 0x34
	if err := f.PutAppStateVersion(ctx, "regular_low", 100, hash); err != nil {
		t.Fatal(err)
	}
	seeded := []store.AppStateMutationMAC{
		{IndexMAC: testBackupMAC(0x01), ValueMAC: testBackupMAC(0x02)},
		{IndexMAC: testBackupMAC(0x03), ValueMAC: testBackupMAC(0x04)},
	}
	if err := f.PutAppStateMutationMACs(ctx, "regular_low", 100, seeded); err != nil {
		t.Fatal(err)
	}

	backup, err := captureAppStateVersion(ctx, f, "regular_low")
	if err != nil || backup == nil {
		t.Fatalf("capture: %v (backup=%v)", err, backup)
	}
	if backup.Version != 100 || backup.Hash != hash || len(backup.MACs) != 2 {
		t.Fatalf("captured %d MACs at v%d, want 2 at v100", len(backup.MACs), backup.Version)
	}

	// Full sync: delete, store partial state, then fail and restore.
	if err = f.DeleteAppStateVersion(ctx, "regular_low"); err != nil {
		t.Fatal(err)
	}
	if err = f.PutAppStateMutationMACs(ctx, "regular_low", 3, []store.AppStateMutationMAC{{IndexMAC: testBackupMAC(0x05), ValueMAC: testBackupMAC(0x06)}}); err != nil {
		t.Fatal(err)
	}
	restored, err := restoreAppStateVersionIfOlder(ctx, f, "regular_low", backup)
	if err != nil {
		t.Fatalf("restore: %v", err)
	}
	if !restored {
		t.Fatal("restore was skipped with no newer state present")
	}
	version, gotHash, err := f.GetAppStateVersion(ctx, "regular_low")
	if err != nil || version != 100 || gotHash != hash {
		t.Fatalf("restored v%d hash[0]=%#x, want v100 hash[0]=0x12 (err=%v)", version, gotHash[0], err)
	}
	for _, m := range seeded {
		if got, _ := f.GetAppStateMutationMAC(ctx, "regular_low", m.IndexMAC); string(got) != string(m.ValueMAC) {
			t.Errorf("restored value MAC for %x = %x, want %x", m.IndexMAC[:2], got, m.ValueMAC[:2])
		}
	}
	if leftover, _ := f.GetAppStateMutationMAC(ctx, "regular_low", testBackupMAC(0x05)); leftover != nil {
		t.Error("partial state of the failed sync survived the restore")
	}
}

// A failed full sync must not restore its pre-sync capture over a state that a
// recovery response adopted (at an equal or newer version) while the sync was
// running; it must still restore when nothing newer was written. This is the
// store-level half of the fetchAppState/handleAppStateRecovery race fix (the
// other half is that both sections run under appStateRecoveryLock). Fails
// without restoreAppStateVersionIfOlder.
func TestRestoreSkippedWhenRecoveryAdoptedMidSync(t *testing.T) {
	ctx := context.Background()
	seed := func(f *fakeAppStateVersionStore, version uint64, hashByte byte, macByte byte) {
		var hash [128]byte
		hash[0] = hashByte
		_ = f.PutAppStateVersion(ctx, "regular_low", version, hash)
		_ = f.PutAppStateMutationMACs(ctx, "regular_low", version, []store.AppStateMutationMAC{{IndexMAC: testBackupMAC(macByte), ValueMAC: testBackupMAC(macByte + 1)}})
	}

	for _, tc := range []struct {
		name            string
		recoveryVersion uint64 // 0 = no recovery arrives mid-sync
		wantRestored    bool
		wantVersion     uint64
	}{
		{"NoRecovery", 0, true, 100},
		{"RecoverySameVersion", 100, false, 100},
		{"RecoveryNewerVersion", 101, false, 101},
		{"RecoveryOlderVersion", 99, true, 100},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFakeAppStateVersionStore()
			seed(f, 100, 0x01, 0x10)

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
				if got, _ := f.GetAppStateMutationMAC(ctx, "regular_low", testBackupMAC(0x10)); got == nil {
					t.Errorf("restored MACs missing")
				}
			} else {
				// The adopted state must be untouched.
				if hash[0] != 0x02 {
					t.Errorf("adopted hash was overwritten")
				}
				if got, _ := f.GetAppStateMutationMAC(ctx, "regular_low", testBackupMAC(0x20)); got == nil {
					t.Errorf("adopted MACs were overwritten")
				}
			}
		})
	}
}
