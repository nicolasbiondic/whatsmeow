// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"testing"

	"google.golang.org/protobuf/proto"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/proto/waSyncdSnapshotRecovery"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
)

type memAppStateStore struct {
	version uint64
	hash    [128]byte
	macs    map[string][]byte
}

func newMemAppStateStore() *memAppStateStore {
	return &memAppStateStore{macs: make(map[string][]byte)}
}

func (f *memAppStateStore) PutAppStateVersion(ctx context.Context, name string, version uint64, hash [128]byte) error {
	f.version, f.hash = version, hash
	return nil
}

func (f *memAppStateStore) GetAppStateVersion(ctx context.Context, name string) (uint64, [128]byte, error) {
	return f.version, f.hash, nil
}

func (f *memAppStateStore) DeleteAppStateVersion(ctx context.Context, name string) error {
	f.version, f.hash = 0, [128]byte{}
	f.macs = make(map[string][]byte)
	return nil
}

func (f *memAppStateStore) PutAppStateMutationMACs(ctx context.Context, name string, version uint64, mutations []store.AppStateMutationMAC) error {
	for _, m := range mutations {
		f.macs[string(m.IndexMAC)] = m.ValueMAC
	}
	return nil
}

func (f *memAppStateStore) DeleteAppStateMutationMACs(ctx context.Context, name string, indexMACs [][]byte) error {
	for _, mac := range indexMACs {
		delete(f.macs, string(mac))
	}
	return nil
}

func (f *memAppStateStore) GetAppStateMutationMAC(ctx context.Context, name string, indexMAC []byte) ([]byte, error) {
	return f.macs[string(indexMAC)], nil
}

type memAppStateKeyStore struct {
	keys map[string]store.AppStateSyncKey
}

func (f *memAppStateKeyStore) PutAppStateSyncKey(ctx context.Context, id []byte, key store.AppStateSyncKey) error {
	f.keys[string(id)] = key
	return nil
}

func (f *memAppStateKeyStore) GetAppStateSyncKey(ctx context.Context, id []byte) (*store.AppStateSyncKey, error) {
	key, ok := f.keys[string(id)]
	if !ok {
		return nil, nil
	}
	return &key, nil
}

func (f *memAppStateKeyStore) GetLatestAppStateSyncKeyID(ctx context.Context) ([]byte, error) {
	for id := range f.keys {
		return []byte(id), nil
	}
	return nil, nil
}

func (f *memAppStateKeyStore) GetAllAppStateSyncKeys(ctx context.Context) ([]*store.AppStateSyncKey, error) {
	out := make([]*store.AppStateSyncKey, 0, len(f.keys))
	for id := range f.keys {
		key := f.keys[id]
		out = append(out, &key)
	}
	return out, nil
}

// buildRecoveryResult builds a syncd fatal recovery response for regular_low
// with one archive record, the way ParseRecovery expects it on the wire.
func buildRecoveryResult(t *testing.T, keyID []byte, version uint64) (*waE2E.PeerDataOperationRequestResponseMessage_PeerDataOperationResult, [128]byte) {
	t.Helper()
	var lthash [128]byte
	if _, err := rand.Read(lthash[:]); err != nil {
		t.Fatal(err)
	}
	index, err := json.Marshal([]string{"archive", "100000000000001@lid"})
	if err != nil {
		t.Fatal(err)
	}
	valueMAC := make([]byte, 32)
	if _, err = rand.Read(valueMAC); err != nil {
		t.Fatal(err)
	}
	recovery := &waSyncdSnapshotRecovery.SyncdSnapshotRecovery{
		Version:        &waSyncdSnapshotRecovery.SyncdVersion{Version: proto.Uint64(version)},
		CollectionName: proto.String("regular_low"),
		MutationRecords: []*waSyncdSnapshotRecovery.SyncdPlainTextRecord{{
			Value: &waSyncAction.SyncActionData{
				Index: index,
				Value: &waSyncAction.SyncActionValue{
					ArchiveChatAction: &waSyncAction.ArchiveChatAction{Archived: proto.Bool(true)},
				},
				Version: proto.Int32(3),
			},
			KeyID: keyID,
			Mac:   valueMAC,
		}},
		CollectionLthash: lthash[:],
	}
	blob, err := proto.Marshal(recovery)
	if err != nil {
		t.Fatal(err)
	}
	return &waE2E.PeerDataOperationRequestResponseMessage_PeerDataOperationResult{
		SyncdSnapshotFatalRecoveryResponse: &waE2E.PeerDataOperationRequestResponseMessage_PeerDataOperationResult_SyncDSnapshotFatalRecoveryResponse{
			CollectionSnapshot: blob,
			IsCompressed:       proto.Bool(false),
		},
	}, lthash
}

func newRecoveryTestClient(t *testing.T) (*Client, *memAppStateStore) {
	t.Helper()
	appStateStore := newMemAppStateStore()
	keyID := []byte{0, 0, 0, 0, 0, 1}
	keyData := make([]byte, 32)
	if _, err := rand.Read(keyData); err != nil {
		t.Fatal(err)
	}
	device := &store.Device{
		ID:           &types.JID{User: "15550001234", Device: 1, Server: types.DefaultUserServer},
		AppState:     appStateStore,
		AppStateKeys: &memAppStateKeyStore{keys: map[string]store.AppStateSyncKey{string(keyID): {Data: keyData}}},
	}
	return NewClient(device, nil), appStateStore
}

func TestHandleAppStateRecovery(t *testing.T) {
	keyID := []byte{0, 0, 0, 0, 0, 1}
	for _, tc := range []struct {
		name            string
		localVersion    uint64
		recoveryVersion uint64
		wantAdopted     bool
	}{
		// The request is only sent when the local state does not verify, so a
		// response at the same version must replace the local LTHash and MACs.
		{"SameVersionAdopted", 100, 100, true},
		{"NewerVersionAdopted", 100, 101, true},
		{"OlderVersionIgnored", 100, 99, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			cli, appStateStore := newRecoveryTestClient(t)
			var localHash [128]byte
			localHash[0] = 0x42
			if err := appStateStore.PutAppStateVersion(ctx, "regular_low", tc.localVersion, localHash); err != nil {
				t.Fatal(err)
			}
			result, recoveryHash := buildRecoveryResult(t, keyID, tc.recoveryVersion)

			cli.handleAppStateRecovery(ctx, "test-req-id", []*waE2E.PeerDataOperationRequestResponseMessage_PeerDataOperationResult{result})

			version, hash, err := appStateStore.GetAppStateVersion(ctx, "regular_low")
			if err != nil {
				t.Fatal(err)
			}
			if tc.wantAdopted {
				if version != tc.recoveryVersion || hash != recoveryHash {
					t.Fatalf("recovery not adopted: local state is v%d hash[0]=%#x, want v%d with the recovery hash", version, hash[0], tc.recoveryVersion)
				}
				if len(appStateStore.macs) != 1 {
					t.Fatalf("stored %d mutation MACs, want 1", len(appStateStore.macs))
				}
			} else {
				if version != tc.localVersion || hash != localHash {
					t.Fatalf("local state was overwritten by an older recovery: v%d hash[0]=%#x", version, hash[0])
				}
			}
		})
	}
}
