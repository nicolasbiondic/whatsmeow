// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package appstate

import (
	"context"
	"crypto/rand"
	"errors"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"go.mau.fi/whatsmeow/proto/waServerSync"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"
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

type memKeyStore struct {
	keys map[string]store.AppStateSyncKey
}

func (f *memKeyStore) PutAppStateSyncKey(ctx context.Context, id []byte, key store.AppStateSyncKey) error {
	f.keys[string(id)] = key
	return nil
}

func (f *memKeyStore) GetAppStateSyncKey(ctx context.Context, id []byte) (*store.AppStateSyncKey, error) {
	key, ok := f.keys[string(id)]
	if !ok {
		return nil, nil
	}
	return &key, nil
}

func (f *memKeyStore) GetLatestAppStateSyncKeyID(ctx context.Context) ([]byte, error) {
	for id := range f.keys {
		return []byte(id), nil
	}
	return nil, nil
}

func (f *memKeyStore) GetAllAppStateSyncKeys(ctx context.Context) ([]*store.AppStateSyncKey, error) {
	out := make([]*store.AppStateSyncKey, 0, len(f.keys))
	for id := range f.keys {
		key := f.keys[id]
		out = append(out, &key)
	}
	return out, nil
}

type toleranceEnv struct {
	proc    *Processor
	keyID   []byte
	keyData []byte
	state   HashState
}

func newToleranceEnv(t *testing.T) *toleranceEnv {
	t.Helper()
	keyID := []byte{0, 0, 0, 0, 0, 1}
	keyData := make([]byte, 32)
	if _, err := rand.Read(keyData); err != nil {
		t.Fatal(err)
	}
	device := &store.Device{
		AppState:     newMemAppStateStore(),
		AppStateKeys: &memKeyStore{keys: map[string]store.AppStateSyncKey{string(keyID): {Data: keyData}}},
	}
	return &toleranceEnv{proc: NewProcessor(device, waLog.Noop), keyID: keyID, keyData: keyData}
}

// encodeTestPatch encodes patchInfo on top of base, the way the author of the
// patch would, and stamps the version the server would assign.
func encodeTestPatch(t *testing.T, env *toleranceEnv, base HashState, patchInfo PatchInfo) *waServerSync.SyncdPatch {
	t.Helper()
	encoded, err := env.proc.EncodePatch(context.Background(), env.keyID, base, patchInfo)
	if err != nil {
		t.Fatalf("EncodePatch: %v", err)
	}
	var patch waServerSync.SyncdPatch
	if err = proto.Unmarshal(encoded, &patch); err != nil {
		t.Fatalf("unmarshal patch: %v", err)
	}
	patch.Version = &waServerSync.SyncdVersion{Version: proto.Uint64(base.Version + 1)}
	return &patch
}

// stateAfter applies patch to base with local semantics, for patches that only
// touch indexes without a previous value.
func stateAfter(t *testing.T, base HashState, patch *waServerSync.SyncdPatch) HashState {
	t.Helper()
	next := base
	next.Version = patch.GetVersion().GetVersion()
	if _, err := next.updateHash(patch.GetMutations(), func([]byte, int) ([]byte, error) { return nil, nil }); err != nil {
		t.Fatalf("updateHash: %v", err)
	}
	return next
}

// divergedHashPatch returns a patch whose author signed the snapshot MAC with
// a base hash that differs from the local one, like every patch written by
// another device after the account's chats were migrated to LID.
func divergedHashPatch(t *testing.T, env *toleranceEnv) *waServerSync.SyncdPatch {
	t.Helper()
	diverged := env.state
	diverged.Hash[0] ^= 0xff
	chat := types.JID{User: "100000000000001", Server: types.HiddenUserServer}
	return encodeTestPatch(t, env, diverged, BuildArchive(chat, true, time.Now(), nil))
}

func TestPatchWithDivergedLTHashFailsByDefault(t *testing.T) {
	env := newToleranceEnv(t)
	list := &PatchList{Name: WAPatchRegularLow, Patches: []*waServerSync.SyncdPatch{divergedHashPatch(t, env)}}
	_, _, err := env.proc.DecodePatches(context.Background(), list, env.state, true)
	if !errors.Is(err, ErrMismatchingLTHash) {
		t.Fatalf("DecodePatches without tolerance: err = %v, want ErrMismatchingLTHash", err)
	}
}

// Fails without the tolerance branch in validatePatch.
func TestPatchWithDivergedLTHashAppliedWhenTolerated(t *testing.T) {
	env := newToleranceEnv(t)
	patch := divergedHashPatch(t, env)
	want := stateAfter(t, env.state, patch)

	ctx := WithLTHashMismatchTolerance(context.Background())
	list := &PatchList{Name: WAPatchRegularLow, Patches: []*waServerSync.SyncdPatch{patch}}
	mutations, got, err := env.proc.DecodePatches(ctx, list, env.state, true)
	if err != nil {
		t.Fatalf("DecodePatches with tolerance: %v", err)
	}
	if got != want {
		t.Fatalf("state after tolerated patch = v%d, want v%d with the locally computed hash", got.Version, want.Version)
	}
	// BuildArchive produces an archive and a pin mutation.
	if len(mutations) != 2 {
		t.Fatalf("got %d mutations, want 2", len(mutations))
	}
	version, hash, err := env.proc.Store.AppState.GetAppStateVersion(ctx, string(WAPatchRegularLow))
	if err != nil || version != want.Version || hash != want.Hash {
		t.Fatalf("stored state = v%d (err=%v), want v%d with the locally computed hash", version, err, want.Version)
	}
}

// The tolerance must only cover the LTHash comparison: a patch whose patch MAC
// or mutation MACs don't verify is still rejected.
func TestToleratedPatchStillVerifiesOtherMACs(t *testing.T) {
	tamper := []struct {
		name    string
		tamper  func(env *toleranceEnv, patch *waServerSync.SyncdPatch)
		wantErr error
	}{{
		name:    "PatchMAC",
		tamper:  func(env *toleranceEnv, patch *waServerSync.SyncdPatch) { patch.PatchMAC[0] ^= 0xff },
		wantErr: ErrMismatchingPatchMAC,
	}, {
		name: "ContentMAC",
		tamper: func(env *toleranceEnv, patch *waServerSync.SyncdPatch) {
			// Corrupt one mutation's value MAC and re-sign the patch MAC, so
			// only the per-mutation verification can catch it.
			value := patch.Mutations[0].GetRecord().GetValue().GetBlob()
			value[len(value)-1] ^= 0xff
			keys := expandAppStateKeys(env.keyData)
			patch.PatchMAC = generatePatchMAC(patch, WAPatchRegularLow, keys.PatchMAC, patch.GetVersion().GetVersion())
		},
		wantErr: ErrMismatchingContentMAC,
	}}
	for _, tc := range tamper {
		t.Run(tc.name, func(t *testing.T) {
			env := newToleranceEnv(t)
			patch := divergedHashPatch(t, env)
			tc.tamper(env, patch)
			ctx := WithLTHashMismatchTolerance(context.Background())
			list := &PatchList{Name: WAPatchRegularLow, Patches: []*waServerSync.SyncdPatch{patch}}
			if _, _, err := env.proc.DecodePatches(ctx, list, env.state, true); !errors.Is(err, tc.wantErr) {
				t.Fatalf("DecodePatches with a tampered %s: err = %v, want %v", tc.name, err, tc.wantErr)
			}
		})
	}
}
