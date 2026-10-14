// Patches whose snapshot MAC was signed with a diverged collection hash.
//
// On accounts whose primary migrated chats to LID, every device keeps its own
// collection LTHash: companion devices such as WhatsApp Web keep signing their
// patches with hashes that no longer match the primary's, and the primary
// signs with a hash no companion can reproduce. WhatsApp Web itself never
// treats a PATCH whose snapshot MAC does not verify as fatal: it marks the
// collection isInMacMismatchFatal, applies the patch and stops verifying that
// collection (WAWebSyncdAntiTampering.js). Only a snapshot mismatch is fatal
// there.
//
// This fork therefore applies a patch whose snapshot MAC does not verify with
// the locally computed hash, as WhatsApp Web does, when:
//
//   - the collection is one of the chat-state collections below, and
//   - the author is a companion (non-zero device index, not flagged as sent
//     by the primary), or the author is the primary AND the collection is
//     LID-migrated (see lidwrite.go), since only the migration makes the
//     primary's hash legitimately diverge from ours.
//
// The patch MAC and every mutation MAC are still verified, and a primary
// patch on a non-migrated collection that does not verify still fails as
// before. Set WHATSMEOW_APPSTATE_COMPANION_TOLERANCE=0 to turn this off.
package appstate

import (
	"errors"
	"os"
	"time"

	"google.golang.org/protobuf/proto"

	"go.mau.fi/whatsmeow/proto/waServerSync"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
)

const companionToleranceEnvVar = "WHATSMEOW_APPSTATE_COMPANION_TOLERANCE"

// The appstate_hash_unconfirmed health alarm fires when this many patches in
// a row were applied tolerated (snapshot MAC unverified) without a single
// verified patch in between, and then again every
// hashUnconfirmedAlarmInterval tolerated patches.
const (
	hashUnconfirmedAlarmThreshold = 20
	hashUnconfirmedAlarmInterval  = 100
)

// Only the chat-state collections are covered: they are the ones companions
// write to while handling chats (archive, pin, read, mute).
var companionTolerantCollections = map[WAPatchName]bool{
	WAPatchRegularLow:  true,
	WAPatchRegularHigh: true,
}

func companionToleranceEnabled(name WAPatchName) bool {
	return companionTolerantCollections[name] && os.Getenv(companionToleranceEnvVar) != "0"
}

type patchAuthor struct {
	deviceIndex uint32
	platform    string
	primary     bool
}

func authorOf(patch *waServerSync.SyncdPatch) patchAuthor {
	author := patchAuthor{deviceIndex: patch.GetDeviceIndex(), primary: patch.GetDeviceIndex() == 0}
	if raw := patch.GetClientDebugData(); len(raw) > 0 {
		var debug waSyncAction.PatchDebugData
		if proto.Unmarshal(raw, &debug) == nil {
			author.platform = debug.GetSenderPlatform().String()
			if debug.GetIsSenderPrimary() {
				author.primary = true
			}
		}
	}
	return author
}

// NoteOwnPatch records the version of a patch this client sent, so that its
// echo is not mistaken for the primary confirming the local hash.
func (proc *Processor) NoteOwnPatch(name WAPatchName, version uint64) {
	proc.companionLock.Lock()
	if proc.ownPatches == nil {
		proc.ownPatches = make(map[WAPatchName]map[uint64]bool)
	}
	if proc.ownPatches[name] == nil {
		proc.ownPatches[name] = make(map[uint64]bool)
	}
	proc.ownPatches[name][version] = true
	proc.companionLock.Unlock()
	proc.appendDiagRecord(map[string]any{
		"ts":         time.Now().UTC().Format(time.RFC3339Nano),
		"type":       "own_patch_sent",
		"collection": string(name),
		"version":    version,
	})
}

// noteToleratedPatch records a patch (companion or primary) applied with the
// locally computed hash because its snapshot MAC did not verify, and raises
// the appstate_hash_unconfirmed health alarm when too many tolerated patches
// pile up without any verified one in between.
func (proc *Processor) noteToleratedPatch(name WAPatchName, version uint64, author patchAuthor) {
	proc.companionLock.Lock()
	if proc.companionPending == nil {
		proc.companionPending = make(map[WAPatchName]uint64)
	}
	if proc.companionPending[name] == 0 {
		proc.companionPending[name] = version
	}
	since := proc.companionPending[name]
	if proc.toleratedStreak == nil {
		proc.toleratedStreak = make(map[WAPatchName]int)
	}
	proc.toleratedStreak[name]++
	streak := proc.toleratedStreak[name]
	proc.companionLock.Unlock()
	role := "companion"
	if author.primary {
		role = "primary"
	}
	proc.Log.Infof("Applied %s patch %s v%d (device %d, %s) whose snapshot MAC uses its own diverged hash", role, name, version, author.deviceIndex, author.platform)
	proc.appendDiagRecord(map[string]any{
		"ts":           time.Now().UTC().Format(time.RFC3339Nano),
		"type":         "patch_tolerated",
		"collection":   string(name),
		"version":      version,
		"device_index": author.deviceIndex,
		"platform":     author.platform,
		"primary":      author.primary,
	})
	if streak == hashUnconfirmedAlarmThreshold ||
		(streak > hashUnconfirmedAlarmThreshold && (streak-hashUnconfirmedAlarmThreshold)%hashUnconfirmedAlarmInterval == 0) {
		proc.Log.Warnf("App state %s: %d patches in a row applied without any snapshot MAC confirming the local hash (since v%d)", name, streak, since)
		proc.appendDiagRecord(map[string]any{
			"ts":            time.Now().UTC().Format(time.RFC3339Nano),
			"type":          "appstate_hash_unconfirmed",
			"collection":    string(name),
			"streak":        streak,
			"since_version": since,
		})
	}
}

// noteVerifiedPatch records a patch whose snapshot MAC verified against the
// locally computed hash. The echo of a patch this client sent itself proves
// nothing (we signed it with our own hash) and is not recorded. Any other
// verified patch confirms the local hash and resets the tolerated streak.
func (proc *Processor) noteVerifiedPatch(name WAPatchName, version uint64, author patchAuthor) {
	proc.companionLock.Lock()
	if proc.ownPatches[name][version] {
		delete(proc.ownPatches[name], version)
		proc.companionLock.Unlock()
		return
	}
	since := proc.companionPending[name]
	delete(proc.companionPending, name)
	delete(proc.toleratedStreak, name)
	proc.companionLock.Unlock()
	if since != 0 {
		proc.Log.Infof("Patch %s v%d verified after tolerated patches applied since v%d", name, version, since)
	}
	proc.appendDiagRecord(map[string]any{
		"ts":           time.Now().UTC().Format(time.RFC3339Nano),
		"type":         "patch_verified",
		"collection":   string(name),
		"version":      version,
		"device_index": author.deviceIndex,
		"platform":     author.platform,
		"primary":      author.primary,
	})
}

func (proc *Processor) noteFailedPatch(name WAPatchName, version uint64, author patchAuthor, err error) {
	if !author.primary || !errors.Is(err, ErrMismatchingLTHash) {
		return
	}
	proc.companionLock.Lock()
	since := proc.companionPending[name]
	delete(proc.companionPending, name)
	delete(proc.toleratedStreak, name)
	proc.companionLock.Unlock()
	if since == 0 {
		return
	}
	proc.Log.Errorf("Primary patch %s v%d does not verify after companion patches applied since v%d", name, version, since)
	proc.appendDiagRecord(map[string]any{
		"ts":         time.Now().UTC().Format(time.RFC3339Nano),
		"type":       "primary_patch_failed_after_companion_tolerance",
		"collection": string(name),
		"version":    version,
		"since":      since,
	})
}
