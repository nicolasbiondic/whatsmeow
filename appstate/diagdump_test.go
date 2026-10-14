package appstate

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	"go.mau.fi/whatsmeow/proto/waServerSync"
	"go.mau.fi/whatsmeow/store"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// The .pb dumps of downloaded snapshots/patches are opt-in: they require
// WHATSMEOW_APPSTATE_DIAG_DUMPS=1 in addition to WHATSMEOW_APPSTATE_DIAG_DIR.
// The jsonl diagnostic records keep working with only the directory set.
func TestDumpPatchListForDiagDumpsOptIn(t *testing.T) {
	proc := &Processor{Store: &store.Device{}, Log: waLog.Noop}
	list := &PatchList{
		Name: WAPatchRegularLow,
		Snapshot: &waServerSync.SyncdSnapshot{
			Version: &waServerSync.SyncdVersion{Version: proto.Uint64(905)},
		},
		Patches: []*waServerSync.SyncdPatch{
			{Version: &waServerSync.SyncdVersion{Version: proto.Uint64(906)}},
		},
	}

	countPB := func(t *testing.T, dir string) int {
		t.Helper()
		matches, err := filepath.Glob(filepath.Join(dir, "*.pb"))
		if err != nil {
			t.Fatalf("glob: %v", err)
		}
		return len(matches)
	}

	t.Run("DirOnlyNoDumps", func(t *testing.T) {
		dir := t.TempDir()
		t.Setenv(diagDirEnvVar, dir)
		t.Setenv(diagDumpsEnvVar, "")
		proc.dumpPatchListForDiag(list)
		if n := countPB(t, dir); n != 0 {
			t.Fatalf("expected no .pb dumps without %s=1, found %d", diagDumpsEnvVar, n)
		}
		if _, err := os.Stat(filepath.Join(dir, "appstate-diag.jsonl")); !os.IsNotExist(err) {
			t.Fatalf("expected no dump records in jsonl without %s=1 (stat err=%v)", diagDumpsEnvVar, err)
		}
		// The jsonl diagnostic log itself (patch_verified, alarms, …) must
		// keep working with only the directory set.
		proc.appendDiagRecord(map[string]any{"type": "patch_verified"})
		data, err := os.ReadFile(filepath.Join(dir, "appstate-diag.jsonl"))
		if err != nil {
			t.Fatalf("jsonl record not written without %s=1: %v", diagDumpsEnvVar, err)
		}
		if !strings.Contains(string(data), `"type":"patch_verified"`) {
			t.Fatalf("jsonl missing patch_verified record: %s", data)
		}
	})

	t.Run("DumpsEnabled", func(t *testing.T) {
		dir := t.TempDir()
		t.Setenv(diagDirEnvVar, dir)
		t.Setenv(diagDumpsEnvVar, "1")
		proc.dumpPatchListForDiag(list)
		if n := countPB(t, dir); n != 2 {
			t.Fatalf("expected 2 .pb dumps (snapshot + patch) with %s=1, found %d", diagDumpsEnvVar, n)
		}
		data, err := os.ReadFile(filepath.Join(dir, "appstate-diag.jsonl"))
		if err != nil {
			t.Fatalf("jsonl not written: %v", err)
		}
		if got := strings.Count(string(data), `"type":"dump"`); got != 2 {
			t.Fatalf("expected 2 dump records in jsonl, found %d: %s", got, data)
		}
	})

	t.Run("NonMonitoredCollectionStillSkipped", func(t *testing.T) {
		dir := t.TempDir()
		t.Setenv(diagDirEnvVar, dir)
		t.Setenv(diagDumpsEnvVar, "1")
		other := &PatchList{Name: WAPatchRegular, Snapshot: list.Snapshot}
		proc.dumpPatchListForDiag(other)
		if n := countPB(t, dir); n != 0 {
			t.Fatalf("expected no .pb dumps for non-monitored collection, found %d", n)
		}
	})
}
