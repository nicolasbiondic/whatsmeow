// Tests against read-only copies of real production session stores (see the
// fixc research notes). They are skipped unless the copies exist at the
// default location or at $WHATSMEOW_LIDWRITE_TEST_DATA.
package appstate

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"google.golang.org/protobuf/proto"

	"go.mau.fi/whatsmeow/proto/waServerSync"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"
)

const defaultRealDataDir = "/tmp/opencode/syncd-research/data/fixc"

func openRealDevice(t *testing.T, tenant string) *store.Device {
	t.Helper()
	dataDir := os.Getenv("WHATSMEOW_LIDWRITE_TEST_DATA")
	if dataDir == "" {
		dataDir = defaultRealDataDir
	}
	src := filepath.Join(dataDir, tenant, "session.db")
	if _, err := os.Stat(src); err != nil {
		t.Skipf("real session copy %s not available: %v", src, err)
	}
	// Work on a throwaway copy: the decode step mutates the store.
	dst := filepath.Join(t.TempDir(), "session.db")
	in, err := os.Open(src)
	if err != nil {
		t.Fatalf("open %s: %v", src, err)
	}
	defer in.Close()
	out, err := os.Create(dst)
	if err != nil {
		t.Fatalf("create %s: %v", dst, err)
	}
	if _, err = io.Copy(out, in); err != nil {
		t.Fatalf("copy session: %v", err)
	}
	if err = out.Close(); err != nil {
		t.Fatalf("close copy: %v", err)
	}
	container, err := sqlstore.New(context.Background(), "sqlite3", fmt.Sprintf("file:%s?_foreign_keys=on", dst), waLog.Noop)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	device, err := container.GetFirstDevice(context.Background())
	if err != nil {
		t.Fatalf("get device: %v", err)
	}
	return device
}

// realIndexMAC computes the index MAC of the given plaintext index under the
// given stored key.
func realIndexMAC(t *testing.T, ctx context.Context, proc *Processor, keyID []byte, index []string) []byte {
	t.Helper()
	keyData, err := proc.Store.AppStateKeys.GetAppStateSyncKey(ctx, keyID)
	if err != nil || keyData == nil {
		t.Fatalf("get key %x: %v", keyID, err)
	}
	return indexMACFor(t, keyData.Data, index)
}

// The real "964" account: LID-migrated, with an archive entry for the chat
// 137929267658756@lid under the PN index 51905448512@s.whatsapp.net written
// with an older key, plus a pin_v1 entry under the PN, plus an archive entry
// under the LID with the current key. Builds the archive patch the way
// WhatsApp Web would, applies it with whatsmeow's own processor (validateMACs
// on) to the copied state, and verifies the result. Fails without the change
// (no REMOVE support, no LID expansion).
func TestReal964ArchiveLIDChat(t *testing.T) {
	ctx := context.Background()
	device := openRealDevice(t, "964-694-693-58wb4p")
	proc := NewProcessor(device, waLog.Noop)

	lid := types.JID{User: "137929267658756", Server: types.HiddenUserServer}
	pn := types.JID{User: "51905448512", Server: types.DefaultUserServer}

	latestKeyID, err := device.AppStateKeys.GetLatestAppStateSyncKeyID(ctx)
	if err != nil || latestKeyID == nil {
		t.Fatalf("latest key ID: %v", err)
	}
	version, hash, err := device.AppState.GetAppStateVersion(ctx, string(WAPatchRegularLow))
	if err != nil {
		t.Fatalf("app state version: %v", err)
	}
	t.Logf("regular_low v%d, latest key %x", version, latestKeyID)
	state := HashState{Version: version, Hash: hash}

	patch := BuildArchive(lid, true, time.Now(), nil)
	expanded, err := proc.ExpandPatchForLID(ctx, patch)
	if err != nil {
		t.Fatalf("ExpandPatchForLID: %v", err)
	}
	var removes []MutationInfo
	for _, m := range expanded.Mutations {
		t.Logf("mutation: %s", mutationSummary(m))
		if m.Operation == waServerSync.SyncdMutation_REMOVE {
			removes = append(removes, m)
		}
	}
	if len(removes) != 2 {
		t.Fatalf("expected 2 REMOVE mutations (archive + pin_v1 PN twins), got %d", len(removes))
	}
	type twin struct {
		indexMAC []byte
		valueMAC []byte
	}
	seen := map[string]bool{}
	var twins []twin
	for _, m := range removes {
		seen[m.Index[0]] = true
		if m.Index[1] != pn.String() {
			t.Errorf("REMOVE index JID is %s, expected %s", m.Index[1], pn)
		}
		if string(m.KeyID) == string(latestKeyID) {
			t.Errorf("REMOVE for %v uses the latest key %x, expected an older one", m.Index, latestKeyID)
		}
		if hex.EncodeToString(m.KeyID) != "00000000f3a0" {
			t.Errorf("REMOVE for %v uses key %x, expected the known old key 00000000f3a0", m.Index, m.KeyID)
		}
		indexMAC := realIndexMAC(t, ctx, proc, m.KeyID, m.Index)
		valueMAC, err := device.AppState.GetAppStateMutationMAC(ctx, string(WAPatchRegularLow), indexMAC)
		if err != nil || valueMAC == nil {
			t.Fatalf("PN twin %v not found in local MAC store before the patch: %v", m.Index, err)
		}
		twins = append(twins, twin{indexMAC: indexMAC, valueMAC: valueMAC})
	}
	if !seen[IndexArchive] || !seen[IndexPin] {
		t.Fatalf("expected REMOVEs for archive and pin_v1, got %v", seen)
	}

	lidArchiveMAC := realIndexMAC(t, ctx, proc, latestKeyID, []string{IndexArchive, lid.String()})
	oldLIDValueMAC, err := device.AppState.GetAppStateMutationMAC(ctx, string(WAPatchRegularLow), lidArchiveMAC)
	if err != nil || oldLIDValueMAC == nil {
		t.Fatalf("LID archive entry not found before the patch: %v", err)
	}

	encoded, err := proc.EncodePatch(ctx, latestKeyID, state, expanded)
	if err != nil {
		t.Fatalf("EncodePatch: %v", err)
	}
	var sp waServerSync.SyncdPatch
	if err = proto.Unmarshal(encoded, &sp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	sp.Version = &waServerSync.SyncdVersion{Version: proto.Uint64(version + 1)}
	list := &PatchList{Name: WAPatchRegularLow, Patches: []*waServerSync.SyncdPatch{&sp}}

	// Apply with whatsmeow's own processor, with full MAC validation: the
	// snapshot MAC only verifies if the LTHash subtracted the stored value
	// MACs of both PN twins and of the overwritten LID entry.
	mutations, newState, err := proc.DecodePatches(ctx, list, state, true)
	if err != nil {
		t.Fatalf("DecodePatches with validateMACs failed: %v", err)
	}
	if newState.Version != version+1 {
		t.Errorf("expected version %d, got %d", version+1, newState.Version)
	}
	if len(mutations) != 4 {
		t.Errorf("expected 4 decoded mutations, got %d", len(mutations))
	}

	// PN twins must be gone from the local MAC store.
	for i, tw := range twins {
		valueMAC, err := device.AppState.GetAppStateMutationMAC(ctx, string(WAPatchRegularLow), tw.indexMAC)
		if err != nil {
			t.Fatalf("lookup twin %d: %v", i, err)
		}
		if valueMAC != nil {
			t.Errorf("PN twin %d still present in local MAC store after the patch", i)
		}
	}
	// The LID archive entry must have been updated to the new value MAC.
	newLIDValueMAC, err := device.AppState.GetAppStateMutationMAC(ctx, string(WAPatchRegularLow), lidArchiveMAC)
	if err != nil || newLIDValueMAC == nil {
		t.Fatalf("LID archive entry missing after the patch: %v", err)
	}
	if string(newLIDValueMAC) == string(oldLIDValueMAC) {
		t.Errorf("LID archive entry was not updated")
	}
}

// The real "Notificaciones" account is NOT LID-migrated (it knows LID
// mappings and has PN-indexed rows, but zero LID-indexed chat entries).
// Every chat write must stay exactly as today.
func TestRealNotificacionesUnchanged(t *testing.T) {
	ctx := context.Background()
	device := openRealDevice(t, "notificaciones-lenz-dbpwff")
	proc := NewProcessor(device, waLog.Noop)

	getter, ok := device.LIDs.(interface {
		GetAllLIDMappings(ctx context.Context) ([]store.LIDMapping, error)
	})
	if !ok {
		t.Fatal("LID store does not support listing mappings")
	}
	mappings, err := getter.GetAllLIDMappings(ctx)
	if err != nil {
		t.Fatalf("list mappings: %v", err)
	}
	if len(mappings) == 0 {
		t.Fatal("expected the account to know some LID mappings")
	}
	for _, mapping := range mappings {
		for _, target := range []types.JID{mapping.PN, mapping.LID} {
			patch := BuildArchive(target, true, time.Now(), nil)
			expanded, err := proc.ExpandPatchForLID(ctx, patch)
			if err != nil {
				t.Fatalf("ExpandPatchForLID(%s): %v", target, err)
			}
			if !reflect.DeepEqual(expanded.Mutations, patch.Mutations) {
				t.Errorf("patch for %s was modified on the non-migrated account", target)
			}
		}
	}
}
