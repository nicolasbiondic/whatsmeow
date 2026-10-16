package appstate

import (
	"context"
	"testing"

	"google.golang.org/protobuf/proto"

	"go.mau.fi/whatsmeow/proto/waServerSync"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// contactWriteEnv is a critical_unblock_low state shaped like the live lines:
// the target contact's row was written under an app state key that has since
// been rotated out (env.oldKeyID), another contact's row is under the latest
// key (env.newKeyID), and the account has NO LID-indexed chat entries (the
// contact path must not depend on LID migration).
type contactWriteEnv struct {
	*lidWriteEnv
	target      types.JID
	other       types.JID
	vTarget     []byte
	vOther      []byte
	targetUnder []byte // key ID the target row is stored under
}

func newContactWriteEnv(t *testing.T, targetUnderLatest bool) *contactWriteEnv {
	t.Helper()
	base := &lidWriteEnv{
		appState: newMemAppStateStore(),
		keyStore: newFakeKeyStore(),
		lids:     newFakeLIDStore(),
		oldKeyID: []byte{0, 0, 0, 0, 0xc1, 0x01},
		newKeyID: []byte{0, 0, 0, 0, 0xc1, 0x02},
	}
	env := &contactWriteEnv{
		lidWriteEnv: base,
		target:      types.JID{User: "51999000111", Server: types.DefaultUserServer},
		other:       types.JID{User: "51999000222", Server: types.DefaultUserServer},
	}
	oldKeyData := randBytes(t, 32)
	newKeyData := randBytes(t, 32)
	base.keyStore.addKey(base.oldKeyID, oldKeyData)
	base.keyStore.addKey(base.newKeyID, newKeyData) // latest
	targetKeyData := oldKeyData
	env.targetUnder = base.oldKeyID
	if targetUnderLatest {
		targetKeyData = newKeyData
		env.targetUnder = base.newKeyID
	}
	env.vTarget = randBytes(t, 32)
	env.vOther = randBytes(t, 32)
	ctx := context.Background()
	if err := base.appState.PutAppStateMutationMACs(ctx, string(WAPatchCriticalUnblockLow), 50, []store.AppStateMutationMAC{
		{IndexMAC: indexMACFor(t, targetKeyData, []string{IndexContact, env.target.String()}), ValueMAC: env.vTarget},
		{IndexMAC: indexMACFor(t, newKeyData, []string{IndexContact, env.other.String()}), ValueMAC: env.vOther},
	}); err != nil {
		t.Fatalf("seed MACs: %v", err)
	}
	base.state = buildState(50, [][]byte{env.vTarget, env.vOther})
	if err := base.appState.PutAppStateVersion(ctx, string(WAPatchCriticalUnblockLow), base.state.Version, base.state.Hash); err != nil {
		t.Fatalf("seed version: %v", err)
	}
	base.proc = NewProcessor(&store.Device{
		AppState:     base.appState,
		AppStateKeys: base.keyStore,
		LIDs:         base.lids,
	}, waLog.Noop)
	return env
}

func (env *contactWriteEnv) rows() []waWebRow {
	return []waWebRow{
		{index: []string{IndexContact, env.target.String()}, keyData: env.keyStore.keys[string(env.targetUnder)].Data, valueMAC: env.vTarget},
		{index: []string{IndexContact, env.other.String()}, keyData: env.keyStore.keys[string(env.newKeyID)].Data, valueMAC: env.vOther},
	}
}

// buildContactSet mirrors wacli's BuildContactPatch (version 2, index by PN).
func buildContactSet(target types.JID, fullName string) PatchInfo {
	return PatchInfo{
		Type: WAPatchCriticalUnblockLow,
		Mutations: []MutationInfo{{
			Index:   []string{IndexContact, target.String()},
			Version: 2,
			Value: &waSyncAction.SyncActionValue{ContactAction: &waSyncAction.ContactAction{
				FullName:                 proto.String(fullName),
				SaveOnPrimaryAddressbook: proto.Bool(true),
			}},
		}},
	}
}

// Renaming a contact whose row is stored under a rotated-out key must carry
// a REMOVE of that index under the old key, after the SET, like WhatsApp
// Web's _generateMutationsToUpload. Fails without expandRotationCleanup.
func TestExpandPatchForLID_ContactRotationRemove(t *testing.T) {
	env := newContactWriteEnv(t, false)
	patch := buildContactSet(env.target, "Test Name")
	expanded, err := env.proc.ExpandPatchForLID(context.Background(), patch)
	if err != nil {
		t.Fatalf("ExpandPatchForLID: %v", err)
	}
	if len(expanded.Mutations) != 2 {
		for _, m := range expanded.Mutations {
			t.Logf("mutation: %s", mutationSummary(m))
		}
		t.Fatalf("expected SET + REMOVE, got %d mutations", len(expanded.Mutations))
	}
	assertNoRepeatedIndexPerOperation(t, expanded.Mutations)
	set, rm := expanded.Mutations[0], expanded.Mutations[1]
	if set.Operation != waServerSync.SyncdMutation_SET || len(set.KeyID) != 0 ||
		set.Index[0] != IndexContact || set.Index[1] != env.target.String() {
		t.Fatalf("first mutation should be the unchanged contact SET, got %s", mutationSummary(set))
	}
	if rm.Operation != waServerSync.SyncdMutation_REMOVE ||
		rm.Index[0] != IndexContact || rm.Index[1] != env.target.String() {
		t.Fatalf("second mutation should REMOVE the contact index, got %s", mutationSummary(rm))
	}
	if string(rm.KeyID) != string(env.oldKeyID) {
		t.Fatalf("REMOVE should use the stored row's old key %x, got %x", env.oldKeyID, rm.KeyID)
	}
	if rm.Version != set.Version {
		t.Fatalf("REMOVE version %d should match the SET version %d", rm.Version, set.Version)
	}
	if len(patch.Mutations) != 1 {
		t.Fatalf("ExpandPatchForLID must not alias the input patch (now %d mutations)", len(patch.Mutations))
	}
}

// Contacts stored under the latest key, contacts with no stored row, and
// contacts stored under two old keys (ambiguous) are sent unchanged.
func TestExpandPatchForLID_ContactUnchangedCases(t *testing.T) {
	ctx := context.Background()
	t.Run("LatestKey", func(t *testing.T) {
		env := newContactWriteEnv(t, true)
		assertContactPatchUnchanged(t, env, buildContactSet(env.target, "Test Name"))
	})
	t.Run("NewContact", func(t *testing.T) {
		env := newContactWriteEnv(t, false)
		fresh := types.JID{User: "51999000333", Server: types.DefaultUserServer}
		assertContactPatchUnchanged(t, env, buildContactSet(fresh, "Test Name"))
	})
	t.Run("AmbiguousOldKeys", func(t *testing.T) {
		env := newContactWriteEnv(t, false)
		_, thirdKeyData := rotateKey(t, env.lidWriteEnv)
		// The target row also exists under a third key, then a fourth key
		// becomes the latest: the index is under two old keys.
		if err := env.appState.PutAppStateMutationMACs(ctx, string(WAPatchCriticalUnblockLow), 51, []store.AppStateMutationMAC{{
			IndexMAC: indexMACFor(t, thirdKeyData, []string{IndexContact, env.target.String()}),
			ValueMAC: randBytes(t, 32),
		}}); err != nil {
			t.Fatal(err)
		}
		env.keyStore.addKey([]byte{0, 0, 0, 0, 0xc1, 0x09}, randBytes(t, 32))
		assertContactPatchUnchanged(t, env, buildContactSet(env.target, "Test Name"))
	})
	t.Run("OtherCollectionsUntouched", func(t *testing.T) {
		env := newContactWriteEnv(t, false)
		patch := buildContactSet(env.target, "Test Name")
		patch.Type = WAPatchCriticalBlock
		assertContactPatchUnchanged(t, env, patch)
	})
}

func assertContactPatchUnchanged(t *testing.T, env *contactWriteEnv, patch PatchInfo) {
	t.Helper()
	expanded, err := env.proc.ExpandPatchForLID(context.Background(), patch)
	if err != nil {
		t.Fatalf("ExpandPatchForLID: %v", err)
	}
	if len(expanded.Mutations) != len(patch.Mutations) {
		for _, m := range expanded.Mutations {
			t.Logf("mutation: %s", mutationSummary(m))
		}
		t.Fatalf("expected the original %d mutations, got %d", len(patch.Mutations), len(expanded.Mutations))
	}
	for i := range patch.Mutations {
		if mutationSummary(expanded.Mutations[i]) != mutationSummary(patch.Mutations[i]) {
			t.Fatalf("mutation %d changed: %s != %s", i, mutationSummary(expanded.Mutations[i]), mutationSummary(patch.Mutations[i]))
		}
	}
}

// encodeContactPatch encodes the patch under the latest key, cross-checks the
// sender hash through the real decoder with MAC validation, and returns the
// encoded mutations and the sender state.
func encodeContactPatch(t *testing.T, env *contactWriteEnv, patch PatchInfo) ([]encodedMutation, HashState) {
	t.Helper()
	ctx := context.Background()
	muts, senderState, sp := encodeAndExtract(t, env.lidWriteEnv, env.newKeyID, patch)
	sp.Version = &waServerSync.SyncdVersion{Version: proto.Uint64(env.state.Version + 1)}
	list := &PatchList{Name: WAPatchCriticalUnblockLow, Patches: []*waServerSync.SyncdPatch{sp}}
	_, newState, err := env.proc.DecodePatches(ctx, list, env.state, true)
	if err != nil {
		t.Fatalf("DecodePatches: %v", err)
	}
	if newState.Hash != senderState.Hash {
		t.Fatalf("decoder state and sender state disagree")
	}
	return muts, senderState
}

// The LTHash whatsmeow computes for a contact rename of a row stored under a
// rotated-out key must equal what WhatsApp Web computes in BOTH lookup modes
// (by index MAC and by plaintext index). Fails without the REMOVE.
func TestContactWriteLTHashMatchesWAWebBothModes(t *testing.T) {
	for _, sc := range []struct {
		name        string
		underLatest bool
	}{
		{"RotatedOutKey", false},
		{"LatestKey", true},
	} {
		t.Run(sc.name, func(t *testing.T) {
			env := newContactWriteEnv(t, sc.underLatest)
			expanded, err := env.proc.ExpandPatchForLID(context.Background(), buildContactSet(env.target, "Test Name"))
			if err != nil {
				t.Fatalf("ExpandPatchForLID: %v", err)
			}
			muts, senderState := encodeContactPatch(t, env, expanded)
			if waWebLTHashByIndexMAC(env.state.Hash, env.rows(), muts) != senderState.Hash {
				t.Errorf("WA Web by-index-MAC hash differs from whatsmeow's")
			}
			if waWebLTHashByPlaintextIndex(env.state.Hash, env.rows(), muts) != senderState.Hash {
				t.Errorf("WA Web by-plaintext-index hash differs from whatsmeow's")
			}
		})
	}
}

// Non-vacuity: the bare SET (what the fork sent before this change) diverges
// from a plaintext-index WhatsApp Web receiver when the row is under an old key.
func TestContactWriteLTHashDivergesWithoutRotationRemove(t *testing.T) {
	env := newContactWriteEnv(t, false)
	muts, senderState := encodeContactPatch(t, env, buildContactSet(env.target, "Test Name"))
	if waWebLTHashByPlaintextIndex(env.state.Hash, env.rows(), muts) == senderState.Hash {
		t.Fatalf("plaintext-index mode should diverge without the rotation REMOVE; the equivalence test is vacuous")
	}
}
