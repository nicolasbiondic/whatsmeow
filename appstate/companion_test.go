package appstate

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"go.mau.fi/whatsmeow/proto/waServerSync"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// encodeTestPatch encodes patchInfo on top of base, the way the author of
// the patch would, and stamps the version the server would assign and the
// author's device index.
func encodeTestPatch(t *testing.T, env *lidWriteEnv, base HashState, patchInfo PatchInfo, deviceIndex uint32) *waServerSync.SyncdPatch {
	t.Helper()
	encoded, err := env.proc.EncodePatch(context.Background(), env.newKeyID, base, patchInfo)
	if err != nil {
		t.Fatalf("EncodePatch: %v", err)
	}
	var patch waServerSync.SyncdPatch
	if err = proto.Unmarshal(encoded, &patch); err != nil {
		t.Fatalf("unmarshal patch: %v", err)
	}
	patch.Version = &waServerSync.SyncdVersion{Version: proto.Uint64(base.Version + 1)}
	if deviceIndex != 0 {
		patch.DeviceIndex = proto.Uint32(deviceIndex)
		debug, err := proto.Marshal(&waSyncAction.PatchDebugData{
			SenderPlatform:  waSyncAction.PatchDebugData_WEB.Enum(),
			IsSenderPrimary: proto.Bool(false),
		})
		if err != nil {
			t.Fatalf("marshal debug data: %v", err)
		}
		patch.ClientDebugData = debug
	}
	return &patch
}

// stateAfter applies patch to base with local semantics, for patches that
// only touch indexes without a previous value.
func stateAfter(t *testing.T, base HashState, patch *waServerSync.SyncdPatch) HashState {
	t.Helper()
	next := base
	next.Version = patch.GetVersion().GetVersion()
	if _, err := next.updateHash(patch.GetMutations(), func([]byte, int) ([]byte, error) { return nil, nil }); err != nil {
		t.Fatalf("updateHash: %v", err)
	}
	return next
}

func freshChat(user string) types.JID {
	return types.JID{User: user, Server: types.HiddenUserServer}
}

// companionScenario returns a WhatsApp Web patch signed from a diverged base
// hash, followed by a primary patch signed from the hash obtained by applying
// the first one normally.
func companionScenario(t *testing.T, env *lidWriteEnv, name WAPatchName, companionDevice uint32) (*waServerSync.SyncdPatch, *waServerSync.SyncdPatch, HashState) {
	t.Helper()
	diverged := env.state
	diverged.Hash[0] ^= 0xff
	companionPatch := BuildArchive(freshChat("100000000000001"), true, time.Now(), nil)
	companionPatch.Type = name
	first := encodeTestPatch(t, env, diverged, companionPatch, companionDevice)
	afterFirst := stateAfter(t, env.state, first)
	primaryPatch := BuildArchive(freshChat("100000000000002"), true, time.Now(), nil)
	primaryPatch.Type = name
	second := encodeTestPatch(t, env, afterFirst, primaryPatch, 0)
	return first, second, stateAfter(t, afterFirst, second)
}

func TestCompanionPatchWithDivergedHashIsApplied(t *testing.T) {
	t.Setenv(companionToleranceEnvVar, "")
	diagDir := t.TempDir()
	t.Setenv(diagDirEnvVar, diagDir)
	env := newLIDWriteEnv(t, true)
	first, second, want := companionScenario(t, env, WAPatchRegularLow, 25)

	list := &PatchList{Name: WAPatchRegularLow, Patches: []*waServerSync.SyncdPatch{first, second}}
	_, got, err := env.proc.DecodePatches(context.Background(), list, env.state, true)
	if err != nil {
		t.Fatalf("DecodePatches: %v", err)
	}
	if got != want {
		t.Fatalf("state after the companion and primary patches = v%d, want v%d with the primary's hash", got.Version, want.Version)
	}
	diag, err := os.ReadFile(filepath.Join(diagDir, "appstate-diag.jsonl"))
	if err != nil {
		t.Fatalf("read diag log: %v", err)
	}
	for _, record := range []string{`"type":"patch_tolerated"`, `"type":"patch_verified"`} {
		if !strings.Contains(string(diag), record) {
			t.Errorf("diag log lacks %s", record)
		}
	}
}

// A primary patch with a diverged hash on a NON-migrated collection (the
// shape of the "Notificaciones" line) must keep failing exactly as today.
func TestPrimaryPatchWithDivergedHashStillFails(t *testing.T) {
	t.Setenv(companionToleranceEnvVar, "")
	env := newLIDWriteEnv(t, false)
	first, _, _ := companionScenario(t, env, WAPatchRegularLow, 0)
	list := &PatchList{Name: WAPatchRegularLow, Patches: []*waServerSync.SyncdPatch{first}}
	if _, _, err := env.proc.DecodePatches(context.Background(), list, env.state, true); !errors.Is(err, ErrMismatchingLTHash) {
		t.Fatalf("primary patch with a diverged hash: err = %v, want ErrMismatchingLTHash", err)
	}
}

// A primary patch with a diverged hash on a LID-migrated collection is
// applied with the locally computed hash, as WhatsApp Web does (change A).
// Fails without the primary tolerance in validatePatch.
func TestPrimaryPatchWithDivergedHashAppliedWhenLIDMigrated(t *testing.T) {
	t.Setenv(companionToleranceEnvVar, "")
	diagDir := t.TempDir()
	t.Setenv(diagDirEnvVar, diagDir)
	env := newLIDWriteEnv(t, true)
	first, second, want := companionScenario(t, env, WAPatchRegularLow, 0)

	list := &PatchList{Name: WAPatchRegularLow, Patches: []*waServerSync.SyncdPatch{first, second}}
	_, got, err := env.proc.DecodePatches(context.Background(), list, env.state, true)
	if err != nil {
		t.Fatalf("DecodePatches: %v", err)
	}
	if got != want {
		t.Fatalf("state after the tolerated primary patch = v%d, want v%d with the locally computed hash", got.Version, want.Version)
	}
	diag, err := os.ReadFile(filepath.Join(diagDir, "appstate-diag.jsonl"))
	if err != nil {
		t.Fatalf("read diag log: %v", err)
	}
	if !strings.Contains(string(diag), `"type":"patch_tolerated"`) || !strings.Contains(string(diag), `"primary":true`) {
		t.Errorf("diag log lacks a patch_tolerated record with primary=true: %s", diag)
	}
}

// A primary patch with a bad patch MAC must still fail, migrated or not.
func TestPrimaryPatchWithBadPatchMACFails(t *testing.T) {
	t.Setenv(companionToleranceEnvVar, "")
	env := newLIDWriteEnv(t, true)
	first, _, _ := companionScenario(t, env, WAPatchRegularLow, 0)
	first.PatchMAC[0] ^= 0xff
	list := &PatchList{Name: WAPatchRegularLow, Patches: []*waServerSync.SyncdPatch{first}}
	if _, _, err := env.proc.DecodePatches(context.Background(), list, env.state, true); !errors.Is(err, ErrMismatchingPatchMAC) {
		t.Fatalf("primary patch with a bad patch MAC: err = %v, want ErrMismatchingPatchMAC", err)
	}
}

func TestCompanionPatchWithBadPatchMACFails(t *testing.T) {
	t.Setenv(companionToleranceEnvVar, "")
	env := newLIDWriteEnv(t, true)
	first, _, _ := companionScenario(t, env, WAPatchRegularLow, 25)
	first.PatchMAC[0] ^= 0xff
	list := &PatchList{Name: WAPatchRegularLow, Patches: []*waServerSync.SyncdPatch{first}}
	if _, _, err := env.proc.DecodePatches(context.Background(), list, env.state, true); !errors.Is(err, ErrMismatchingPatchMAC) {
		t.Fatalf("companion patch with a bad patch MAC: err = %v, want ErrMismatchingPatchMAC", err)
	}
}

func TestCompanionToleranceCanBeDisabled(t *testing.T) {
	t.Setenv(companionToleranceEnvVar, "0")
	env := newLIDWriteEnv(t, true)
	first, _, _ := companionScenario(t, env, WAPatchRegularLow, 25)
	list := &PatchList{Name: WAPatchRegularLow, Patches: []*waServerSync.SyncdPatch{first}}
	if _, _, err := env.proc.DecodePatches(context.Background(), list, env.state, true); !errors.Is(err, ErrMismatchingLTHash) {
		t.Fatalf("with the tolerance disabled: err = %v, want ErrMismatchingLTHash", err)
	}
}

func TestCompanionToleranceOnlyCoversChatStateCollections(t *testing.T) {
	t.Setenv(companionToleranceEnvVar, "")
	env := newLIDWriteEnv(t, true)
	first, _, _ := companionScenario(t, env, WAPatchRegular, 25)
	list := &PatchList{Name: WAPatchRegular, Patches: []*waServerSync.SyncdPatch{first}}
	if _, _, err := env.proc.DecodePatches(context.Background(), list, env.state, true); !errors.Is(err, ErrMismatchingLTHash) {
		t.Fatalf("companion patch in the regular collection: err = %v, want ErrMismatchingLTHash", err)
	}
}

// Real data from line 964 on 2026-10-12: local state v1655, then two
// WhatsApp Web patches (device 25) with their own diverged hash interleaved
// with primary patches, the last one under a rotated key.
func TestReal964WhatsAppWebPatches(t *testing.T) {
	t.Setenv(companionToleranceEnvVar, "")
	dataDir := os.Getenv("WHATSMEOW_COMPANION_TEST_DATA")
	if dataDir == "" {
		dataDir = "/tmp/opencode/syncd-research/data/companion"
	}
	t.Setenv("WHATSMEOW_LIDWRITE_TEST_DATA", dataDir)
	device := openRealDevice(t, "964")
	ctx := context.Background()
	version, hash, err := device.AppState.GetAppStateVersion(ctx, string(WAPatchRegularLow))
	if err != nil {
		t.Fatalf("get version: %v", err)
	}
	if version != 1655 {
		t.Skipf("real data is at v%d, this test expects v1655", version)
	}
	var patches []*waServerSync.SyncdPatch
	for v := 1656; v <= 1660; v++ {
		raw, err := os.ReadFile(filepath.Join(dataDir, "964", "dumps", "regular_low-v"+strconv.Itoa(v)+".patch.pb"))
		if err != nil {
			t.Skipf("dump v%d not available: %v", v, err)
		}
		var patch waServerSync.SyncdPatch
		if err = proto.Unmarshal(raw, &patch); err != nil {
			t.Fatalf("unmarshal v%d: %v", v, err)
		}
		patches = append(patches, &patch)
	}
	proc := NewProcessor(device, waLog.Noop)
	list := &PatchList{Name: WAPatchRegularLow, Patches: patches}
	_, got, err := proc.DecodePatches(ctx, list, HashState{Version: version, Hash: hash}, true)
	if err != nil {
		t.Fatalf("DecodePatches over the real WhatsApp Web patches: %v", err)
	}
	if got.Version != 1660 {
		t.Fatalf("final version = %d, want 1660", got.Version)
	}
}

// The appstate_hash_unconfirmed health alarm fires after 20 tolerated patches
// in a row without any verified one, then every 100 more; a verified patch
// resets the streak.
func TestHashUnconfirmedAlarm(t *testing.T) {
	diagDir := t.TempDir()
	t.Setenv(diagDirEnvVar, diagDir)
	env := newLIDWriteEnv(t, true)
	primary := patchAuthor{deviceIndex: 0, primary: true}

	countAlarms := func() int {
		raw, err := os.ReadFile(filepath.Join(diagDir, "appstate-diag.jsonl"))
		if err != nil {
			return 0
		}
		return strings.Count(string(raw), `"type":"appstate_hash_unconfirmed"`)
	}

	for v := uint64(1); v <= 19; v++ {
		env.proc.noteToleratedPatch(WAPatchRegularLow, 1000+v, primary)
	}
	if got := countAlarms(); got != 0 {
		t.Fatalf("alarm fired after 19 tolerated patches (%d records)", got)
	}
	env.proc.noteToleratedPatch(WAPatchRegularLow, 1020, primary)
	if got := countAlarms(); got != 1 {
		t.Fatalf("expected 1 alarm after 20 tolerated patches, got %d", got)
	}
	raw, _ := os.ReadFile(filepath.Join(diagDir, "appstate-diag.jsonl"))
	if !strings.Contains(string(raw), `"since_version":1001`) || !strings.Contains(string(raw), `"streak":20`) {
		t.Errorf("alarm record lacks streak/since_version: %s", raw)
	}
	for v := uint64(21); v <= 119; v++ {
		env.proc.noteToleratedPatch(WAPatchRegularLow, 1000+v, primary)
	}
	if got := countAlarms(); got != 1 {
		t.Fatalf("expected no repeat alarm before 120 tolerated patches, got %d", got)
	}
	env.proc.noteToleratedPatch(WAPatchRegularLow, 1120, primary)
	if got := countAlarms(); got != 2 {
		t.Fatalf("expected the repeat alarm at 120 tolerated patches, got %d", got)
	}
	// A verified patch resets the streak: 20 more tolerated ones are needed.
	env.proc.noteVerifiedPatch(WAPatchRegularLow, 1121, primary)
	for v := uint64(1); v <= 19; v++ {
		env.proc.noteToleratedPatch(WAPatchRegularLow, 1121+v, primary)
	}
	if got := countAlarms(); got != 2 {
		t.Fatalf("alarm fired again before 20 post-reset tolerated patches, got %d", got)
	}
	env.proc.noteToleratedPatch(WAPatchRegularLow, 1141, primary)
	if got := countAlarms(); got != 3 {
		t.Fatalf("expected a new alarm 20 tolerated patches after the reset, got %d", got)
	}
}

type diagPatchRecord struct {
	Type        string `json:"type"`
	Collection  string `json:"collection"`
	Version     uint64 `json:"version"`
	DeviceIndex uint32 `json:"device_index"`
	Primary     bool   `json:"primary"`
	Records     int    `json:"records"`
}

func readDiagRecords(t *testing.T, diagDir string) []diagPatchRecord {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(diagDir, "appstate-diag.jsonl"))
	if err != nil {
		t.Fatalf("read diag log: %v", err)
	}
	var out []diagPatchRecord
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if line == "" {
			continue
		}
		var rec diagPatchRecord
		if err := json.Unmarshal([]byte(line), &rec); err != nil {
			t.Fatalf("unmarshal diag line %q: %v", line, err)
		}
		out = append(out, rec)
	}
	return out
}

func companionTestDataDir(t *testing.T) string {
	t.Helper()
	dataDir := os.Getenv("WHATSMEOW_COMPANION_TEST_DATA")
	if dataDir == "" {
		dataDir = "/tmp/opencode/syncd-research/data/companion"
	}
	t.Setenv("WHATSMEOW_LIDWRITE_TEST_DATA", dataDir)
	return dataDir
}

func loadRealPatches(t *testing.T, dataDir, tenant string, from, to int) []*waServerSync.SyncdPatch {
	t.Helper()
	var patches []*waServerSync.SyncdPatch
	for v := from; v <= to; v++ {
		raw, err := os.ReadFile(filepath.Join(dataDir, tenant, "dumps", "regular_low-v"+strconv.Itoa(v)+".patch.pb"))
		if err != nil {
			t.Skipf("dump v%d not available: %v", v, err)
		}
		var patch waServerSync.SyncdPatch
		if err = proto.Unmarshal(raw, &patch); err != nil {
			t.Fatalf("unmarshal v%d: %v", v, err)
		}
		patches = append(patches, &patch)
	}
	return patches
}

// Real data from line 905 on 2026-10-13: local verified state at v956, then
// 27 patches v957..v983 — 8 from WhatsApp Web sessions (devices 23 and 24)
// that verify against our locally computed hash, and 19 from the phone
// (device 0) signed with the primary's own diverged hash, which must be
// tolerated because the collection is LID-migrated. Fails without change A
// (the primary tolerance in validatePatch).
func TestReal905PrimaryPatchesTolerated(t *testing.T) {
	t.Setenv(companionToleranceEnvVar, "")
	diagDir := t.TempDir()
	t.Setenv(diagDirEnvVar, diagDir)
	dataDir := companionTestDataDir(t)
	device := openRealDevice(t, "905")
	ctx := context.Background()
	version, hash, err := device.AppState.GetAppStateVersion(ctx, string(WAPatchRegularLow))
	if err != nil {
		t.Fatalf("get version: %v", err)
	}
	if version != 956 {
		t.Skipf("real data is at v%d, this test expects v956", version)
	}
	patches := loadRealPatches(t, dataDir, "905", 957, 983)
	proc := NewProcessor(device, waLog.Noop)
	list := &PatchList{Name: WAPatchRegularLow, Patches: patches}
	_, got, err := proc.DecodePatches(ctx, list, HashState{Version: version, Hash: hash}, true)
	if err != nil {
		t.Fatalf("DecodePatches over the real 905 patches: %v", err)
	}
	if got.Version != 983 {
		t.Fatalf("final version = %d, want 983", got.Version)
	}
	var verified, tolerated int
	for _, rec := range readDiagRecords(t, diagDir) {
		switch rec.Type {
		case "patch_verified":
			verified++
			if rec.DeviceIndex != 23 && rec.DeviceIndex != 24 {
				t.Errorf("patch v%d verified from unexpected device %d", rec.Version, rec.DeviceIndex)
			}
		case "patch_tolerated":
			tolerated++
			if rec.DeviceIndex != 0 || !rec.Primary {
				t.Errorf("patch v%d tolerated from unexpected device %d (primary=%t)", rec.Version, rec.DeviceIndex, rec.Primary)
			}
		}
	}
	if verified != 8 {
		t.Errorf("patch_verified count = %d, want 8 (the WhatsApp Web patches)", verified)
	}
	if tolerated != 19 {
		t.Errorf("patch_tolerated count = %d, want 19 (the phone patches)", tolerated)
	}
}

// Real data from line 922: no local regular_low state at all, a server
// snapshot v222 whose MAC does not verify against the plain sum of its 1717
// records, and patches v223..v233 each signed with their author's own hash.
// With the bootstrap flag the snapshot is adopted (its records MAC-verify and
// contain LID chat actions) and every patch applies; without it the snapshot
// keeps failing. Fails without change B.
func TestReal922SnapshotBootstrap(t *testing.T) {
	t.Setenv(companionToleranceEnvVar, "")
	diagDir := t.TempDir()
	t.Setenv(diagDirEnvVar, diagDir)
	dataDir := companionTestDataDir(t)
	device := openRealDevice(t, "922")
	ctx := context.Background()
	version, _, err := device.AppState.GetAppStateVersion(ctx, string(WAPatchRegularLow))
	if err != nil {
		t.Fatalf("get version: %v", err)
	}
	if version != 0 {
		t.Skipf("real data has regular_low at v%d, this test expects no local state", version)
	}
	rawSnap, err := os.ReadFile(filepath.Join(dataDir, "922", "dumps", "regular_low-v222.snapshot.pb"))
	if err != nil {
		t.Skipf("snapshot dump not available: %v", err)
	}
	var snap waServerSync.SyncdSnapshot
	if err = proto.Unmarshal(rawSnap, &snap); err != nil {
		t.Fatalf("unmarshal snapshot: %v", err)
	}
	patches := loadRealPatches(t, dataDir, "922", 223, 233)
	list := &PatchList{Name: WAPatchRegularLow, Snapshot: &snap, Patches: patches}

	// Without the bootstrap flag the snapshot must keep failing.
	failProc := NewProcessor(device, waLog.Noop)
	if _, _, err := failProc.DecodePatches(ctx, list, HashState{}, true); !errors.Is(err, ErrMismatchingLTHash) {
		t.Fatalf("without the bootstrap flag: err = %v, want ErrMismatchingLTHash", err)
	}

	proc := NewProcessor(device, waLog.Noop)
	_, got, err := proc.DecodePatches(WithSnapshotBootstrap(ctx), list, HashState{}, true)
	if err != nil {
		t.Fatalf("DecodePatches with the bootstrap flag: %v", err)
	}
	if got.Version != 233 {
		t.Fatalf("final version = %d, want 233", got.Version)
	}
	foundBootstrap := false
	for _, rec := range readDiagRecords(t, diagDir) {
		if rec.Type == "snapshot_bootstrap_tolerated" {
			foundBootstrap = true
			if rec.Version != 222 || rec.Records != len(snap.GetRecords()) {
				t.Errorf("bootstrap record = v%d/%d records, want v222/%d", rec.Version, rec.Records, len(snap.GetRecords()))
			}
		}
	}
	if !foundBootstrap {
		t.Error("diag log lacks the snapshot_bootstrap_tolerated record")
	}
}
