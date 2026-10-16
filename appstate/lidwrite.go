// Outgoing chat-state mutations on PN→LID migrated accounts, written the way
// WhatsApp Web writes them (see also lidrecovery.go for the read side).
//
// On an account whose primary migrated 1:1 chats from phone number (PN) JIDs
// to LID JIDs, WhatsApp Web:
//
//   - writes chat actions (archive, pin_v1, mute, markChatAsRead, …) with the
//     index JID set to the chat's LID (WAWebSyncdGetChat.
//     getChatJidMutationIndexForChat);
//   - if a local row with the same action but the PN JID still exists, adds a
//     REMOVE mutation for that PN index to the SAME patch (WAWebSyncdCoreApi.
//     lockForSync → getPnMutationsForLidCleanup), so the stale entry's value
//     MAC is subtracted from the server's LTHash through the normal protocol;
//   - emits each REMOVE under the key ID the stored row was written with, so
//     the index MAC matches the stored entry
//     (WAWebSyncdRequestBuilderBuild._generateMutationsToUpload).
//
// ExpandPatchForLID reproduces that behavior for whatsmeow. Since whatsmeow
// only stores index MAC → value MAC pairs (no plaintext indices), the PN twin
// is located by computing HMAC(indexKey, index) with every known app state key
// and checking the local MAC store.
//
// The account is only treated as LID-migrated when the local state of the
// collection verifiably contains at least one LID-indexed chat-action entry
// (checked the same way: HMAC over the known LID↔PN mappings with every known
// key). Non-migrated accounts (e.g. ones that merely know some LID mappings)
// and chats without a PN↔LID mapping keep the previous behavior exactly:
// the patch is returned unchanged.
package appstate

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"go.mau.fi/whatsmeow/proto/waServerSync"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
)

// allAppStateSyncKeyIDsGetter is an optional extension of
// store.AppStateSyncKeyStore, implemented by sqlstore in this fork.
type allAppStateSyncKeyIDsGetter interface {
	GetAllAppStateSyncKeyIDs(ctx context.Context) ([][]byte, error)
}

// allLIDMappingsGetter is an optional extension of store.LIDStore,
// implemented by sqlstore in this fork.
type allLIDMappingsGetter interface {
	GetAllLIDMappings(ctx context.Context) ([]store.LIDMapping, error)
}

// chatActionIndexes lists, per collection, the index types whose shape is
// ["action", chatJID] and which WhatsApp Web writes with the chat's LID index
// on LID-migrated accounts. Deliberately restricted to the actions wacli
// sends; other mutations are passed through unchanged.
var chatActionIndexes = map[WAPatchName][]string{
	WAPatchRegularLow:  {IndexArchive, IndexPin, IndexMarkChatAsRead},
	WAPatchRegularHigh: {IndexMute},
}

// rotationCleanupIndexes lists, per collection, index types that are not
// LID-rewritten chat actions but whose outgoing SETs still get WhatsApp Web's
// rotated-out-key cleanup (_generateMutationsToUpload $()), independently of
// LID migration: if the index being SET is stored locally under exactly one
// app state key older than the latest one, a REMOVE of that index under the
// old key is appended to the same patch. Deliberately restricted to the
// mutations wacli sends; everything else is passed through unchanged.
var rotationCleanupIndexes = map[WAPatchName][]string{
	WAPatchCriticalUnblockLow: {IndexContact},
}

type expandedKey struct {
	id   []byte
	keys ExpandedAppStateKeys
}

func (proc *Processor) getAllExpandedKeys(ctx context.Context) ([]expandedKey, error) {
	getter, ok := proc.Store.AppStateKeys.(allAppStateSyncKeyIDsGetter)
	if !ok {
		return nil, nil
	}
	keyIDs, err := getter.GetAllAppStateSyncKeyIDs(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list app state key IDs: %w", err)
	}
	out := make([]expandedKey, 0, len(keyIDs))
	for _, keyID := range keyIDs {
		keys, err := proc.getAppStateKey(ctx, keyID)
		if err != nil {
			proc.Log.Warnf("Skipping app state key %X while searching local state: %v", keyID, err)
			continue
		}
		out = append(out, expandedKey{id: keyID, keys: keys})
	}
	return out, nil
}

type indexMACSet map[[32]byte]struct{}

func (s indexMACSet) contains(mac []byte) bool {
	if len(mac) != 32 {
		return false
	}
	_, ok := s[*(*[32]byte)(mac)]
	return ok
}

// findStoredIndexKeys returns the key IDs under which the given plaintext
// index has a row in the local MAC store (i.e. the keys k for which
// HMAC(indexKey_k, index) is a stored index MAC).
func findStoredIndexKeys(index []string, keys []expandedKey, macs indexMACSet) ([][]byte, error) {
	indexBytes, err := json.Marshal(index)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal index: %w", err)
	}
	var found [][]byte
	for _, k := range keys {
		mac := concatAndHMAC(sha256.New, k.keys.Index, indexBytes)
		if macs.contains(mac) {
			found = append(found, k.id)
		}
	}
	return found, nil
}

// hasLIDChatEntry reports whether the local MAC store contains any entry with
// index ["action", lid] for the given LID, actions and keys.
func hasLIDChatEntry(lid types.JID, actions []string, keys []expandedKey, macs indexMACSet) bool {
	for _, action := range actions {
		found, err := findStoredIndexKeys([]string{action, lid.String()}, keys, macs)
		if err == nil && len(found) > 0 {
			return true
		}
	}
	return false
}

// isCollectionLIDMigrated decides whether the account's syncd state for this
// collection is LID-migrated: it is iff the local state contains at least one
// LID-indexed chat-action entry. The target chats of the patch are checked
// first, then every LID in the local LID map. Positive results are cached on
// the Processor for the lifetime of the process.
func (proc *Processor) isCollectionLIDMigrated(
	ctx context.Context,
	name WAPatchName,
	actions []string,
	targetLIDs []types.JID,
	keys []expandedKey,
	macs indexMACSet,
) bool {
	proc.lidMigratedCacheLock.Lock()
	cached := proc.lidMigratedCache[name]
	proc.lidMigratedCacheLock.Unlock()
	if cached {
		return true
	}
	migrated := false
	for _, lid := range targetLIDs {
		if hasLIDChatEntry(lid, actions, keys, macs) {
			migrated = true
			break
		}
	}
	if !migrated {
		getter, ok := proc.Store.LIDs.(allLIDMappingsGetter)
		if !ok {
			return false
		}
		mappings, err := getter.GetAllLIDMappings(ctx)
		if err != nil {
			proc.Log.Warnf("Failed to list LID mappings while checking %s migration: %v", name, err)
			return false
		}
		for _, mapping := range mappings {
			if hasLIDChatEntry(mapping.LID, actions, keys, macs) {
				migrated = true
				break
			}
		}
	}
	if migrated {
		proc.lidMigratedCacheLock.Lock()
		proc.lidMigratedCache[name] = true
		proc.lidMigratedCacheLock.Unlock()
		proc.Log.Infof("App state collection %s detected as LID-migrated, writing chat actions with LID indexes", name)
	}
	return migrated
}

// isCollectionLIDMigratedForDecode answers isCollectionLIDMigrated from the
// decode path, where no outgoing patch (and thus no target LID list) exists:
// it loads the app state keys and the collection's MAC store itself and
// checks every known LID mapping (targetLIDs = nil). The cached positive
// answer makes repeated calls free.
// IsCollectionLIDMigrated reports whether the local state of the collection
// holds LID-indexed chat entries (see isCollectionLIDMigrated). Positive
// results are cached for the lifetime of the processor.
func (proc *Processor) IsCollectionLIDMigrated(ctx context.Context, name WAPatchName) bool {
	return proc.isCollectionLIDMigratedForDecode(ctx, name)
}

func (proc *Processor) isCollectionLIDMigratedForDecode(ctx context.Context, name WAPatchName) bool {
	actions := chatActionIndexes[name]
	if len(actions) == 0 || proc.Store == nil || proc.Store.LIDs == nil || proc.Store.AppState == nil {
		return false
	}
	proc.lidMigratedCacheLock.Lock()
	cached := proc.lidMigratedCache[name]
	proc.lidMigratedCacheLock.Unlock()
	if cached {
		return true
	}
	macsGetter, ok := proc.Store.AppState.(allAppStateMutationMACsGetter)
	if !ok {
		return false
	}
	keys, err := proc.getAllExpandedKeys(ctx)
	if err != nil {
		proc.Log.Warnf("Failed to load app state keys while checking %s migration: %v", name, err)
		return false
	}
	if len(keys) == 0 {
		return false
	}
	macList, err := macsGetter.GetAllAppStateMutationMACs(ctx, string(name))
	if err != nil {
		proc.Log.Warnf("Failed to list mutation MACs while checking %s migration: %v", name, err)
		return false
	}
	macs := make(indexMACSet, len(macList))
	for _, mac := range macList {
		if len(mac.IndexMAC) == 32 {
			macs[*(*[32]byte)(mac.IndexMAC)] = struct{}{}
		}
	}
	return proc.isCollectionLIDMigrated(ctx, name, actions, nil, keys, macs)
}

// resolvedChatMutation is a chat-action SET mutation with its chat's LID/PN
// pair resolved from the LID store. Either side may be zero if unknown.
type resolvedChatMutation struct {
	lid types.JID
	pn  types.JID
}

func (proc *Processor) resolveChatJID(ctx context.Context, rawJID string) (res resolvedChatMutation) {
	jid, err := types.ParseJID(rawJID)
	if err != nil {
		return
	}
	jid = jid.ToNonAD()
	switch jid.Server {
	case types.HiddenUserServer:
		res.lid = jid
		pn, err := proc.Store.LIDs.GetPNForLID(ctx, jid)
		if err == nil {
			res.pn = pn.ToNonAD()
		}
	case types.DefaultUserServer:
		res.pn = jid
		lid, err := proc.Store.LIDs.GetLIDForPN(ctx, jid)
		if err == nil {
			res.lid = lid.ToNonAD()
		}
	}
	return
}

// ExpandPatchForLID returns a copy of patchInfo with chat-action mutations
// rewritten the way WhatsApp Web writes them on a LID-migrated account:
//
//   - SET mutations for chat actions use the chat's LID as the index JID;
//   - for every PN-twin entry of such a mutation found in the local app state
//     MAC store (searched under every known app state key), a REMOVE mutation
//     with the PN index is appended to the same patch, under the key ID that
//     matches the stored index MAC;
//   - if the SET's own (LID) index is itself stored under an app state key
//     OLDER than the latest one (i.e. it was last written before a key
//     rotation), a REMOVE of that index under its old key is appended after
//     all other mutations, exactly like WhatsApp Web's
//     _generateMutationsToUpload ($()) does for every outgoing SET. Without
//     it the old-key row would stay on the server as a duplicate record, and
//     a receiver in plaintext-index LTHash lookup mode
//     (WAWebSyncdAntiTampering with syncd_use_index_for_lthash_lookup) would
//     subtract the stored row for the SET while we do not, diverging the
//     hashes. An index stored under two or more older keys is ambiguous and
//     is skipped (logged), keeping at most one REMOVE per plaintext index.
//
// For collections without LID-rewritten chat actions, only the rotated-out-key
// cleanup of expandRotationCleanup applies (e.g. contact SETs in
// critical_unblock_low), whether or not the account is LID-migrated.
//
// If the account is not LID-migrated (see isCollectionLIDMigrated), the chat
// has no known PN↔LID mapping, or the patch contains no chat-action
// mutations, the patch is returned unchanged. The returned PatchInfo never
// aliases patchInfo.Mutations, so callers may re-expand the original patch
// (e.g. on retry) without duplicating mutations.
func (proc *Processor) ExpandPatchForLID(ctx context.Context, patchInfo PatchInfo) (PatchInfo, error) {
	actions := chatActionIndexes[patchInfo.Type]
	if len(actions) == 0 {
		return proc.expandRotationCleanup(ctx, patchInfo, rotationCleanupIndexes[patchInfo.Type])
	}
	if proc.Store == nil || proc.Store.LIDs == nil || proc.Store.AppState == nil {
		return patchInfo, nil
	}
	isChatAction := func(m MutationInfo) bool {
		if m.Operation != waServerSync.SyncdMutation_SET || len(m.KeyID) != 0 || len(m.Index) != 2 {
			return false
		}
		for _, action := range actions {
			if m.Index[0] == action {
				return true
			}
		}
		return false
	}
	anyChatAction := false
	for _, m := range patchInfo.Mutations {
		if isChatAction(m) {
			anyChatAction = true
			break
		}
	}
	if !anyChatAction {
		return patchInfo, nil
	}

	macsGetter, ok := proc.Store.AppState.(allAppStateMutationMACsGetter)
	if !ok {
		return patchInfo, nil
	}
	keys, err := proc.getAllExpandedKeys(ctx)
	if err != nil {
		return patchInfo, err
	}
	if len(keys) == 0 {
		return patchInfo, nil
	}
	macList, err := macsGetter.GetAllAppStateMutationMACs(ctx, string(patchInfo.Type))
	if err != nil {
		return patchInfo, fmt.Errorf("failed to list app state mutation MACs: %w", err)
	}
	macs := make(indexMACSet, len(macList))
	for _, mac := range macList {
		if len(mac.IndexMAC) == 32 {
			macs[*(*[32]byte)(mac.IndexMAC)] = struct{}{}
		}
	}

	// Resolve the LID/PN pair of every chat-action mutation.
	resolved := make(map[int]resolvedChatMutation, len(patchInfo.Mutations))
	var targetLIDs []types.JID
	for i, m := range patchInfo.Mutations {
		if !isChatAction(m) {
			continue
		}
		res := proc.resolveChatJID(ctx, m.Index[1])
		resolved[i] = res
		if !res.lid.IsEmpty() {
			targetLIDs = append(targetLIDs, res.lid)
		}
	}
	if len(targetLIDs) == 0 {
		// No mutation has a known LID mapping: keep today's behavior.
		return patchInfo, nil
	}
	if !proc.isCollectionLIDMigrated(ctx, patchInfo.Type, actions, targetLIDs, keys, macs) {
		return patchInfo, nil
	}

	// The key the patch will be encoded with: sendAppState always encodes
	// under GetLatestAppStateSyncKeyID, so rows stored under any other key are
	// "old key" rows for the rotation cleanup below.
	latestKeyID, err := proc.Store.AppStateKeys.GetLatestAppStateSyncKeyID(ctx)
	if err != nil {
		return patchInfo, fmt.Errorf("failed to get latest app state key ID: %w", err)
	}

	out := patchInfo
	out.Mutations = make([]MutationInfo, 0, len(patchInfo.Mutations)+2)
	var removes, rotationRemoves []MutationInfo
	seenRemoves := make(map[string]bool)
	for i, m := range patchInfo.Mutations {
		res, ok := resolved[i]
		if !ok || res.lid.IsEmpty() {
			// Not a chat action, or unknown mapping: unchanged.
			out.Mutations = append(out.Mutations, m)
			continue
		}
		// WA Web getChatJidMutationIndexForChat: on a migrated account the
		// index JID is always the chat's LID.
		m.Index = []string{m.Index[0], res.lid.String()}
		out.Mutations = append(out.Mutations, m)
		// WA Web _generateMutationsToUpload ($()): if the index this SET is
		// about to (re)write still has a row stored under an older key,
		// REMOVE that row under its old key in the same patch. Emitted after
		// every other mutation, in WhatsApp Web's order.
		lidIndex := []string{m.Index[0], m.Index[1]}
		foundLIDKeys, err := findStoredIndexKeys(lidIndex, keys, macs)
		if err != nil {
			return patchInfo, err
		}
		oldKeys := foundLIDKeys[:0:0]
		for _, k := range foundLIDKeys {
			if !bytes.Equal(k, latestKeyID) {
				oldKeys = append(oldKeys, k)
			}
		}
		if len(oldKeys) > 1 {
			// Same ambiguity rule as the PN twins: a second REMOVE of the
			// same plaintext index would be fatal for WhatsApp Web
			// (validateNoSameIndexForMultipleMutations), so leave it alone.
			proc.Log.Warnf("Index %v is stored under %d old app state keys; not removing the stale rows", lidIndex, len(oldKeys))
		} else if len(oldKeys) == 1 {
			dedupKey := lidIndex[0] + "\x00" + lidIndex[1]
			if !seenRemoves[dedupKey] {
				seenRemoves[dedupKey] = true
				proc.Log.Infof("Adding REMOVE of %v under rotated-out key %X to outgoing %s patch", lidIndex, oldKeys[0], patchInfo.Type)
				rotationRemoves = append(rotationRemoves, MutationInfo{
					Index:     lidIndex,
					Version:   m.Version,
					Value:     &waSyncAction.SyncActionValue{},
					Operation: waServerSync.SyncdMutation_REMOVE,
					KeyID:     oldKeys[0],
				})
			}
		}
		if res.pn.IsEmpty() {
			continue
		}
		// WA Web getPnMutationsForLidCleanup: REMOVE the PN-twin row (if any)
		// in the same patch, under the key its index MAC was stored with.
		pnIndex := []string{m.Index[0], res.pn.String()}
		foundKeys, err := findStoredIndexKeys(pnIndex, keys, macs)
		if err != nil {
			return patchInfo, err
		}
		// WhatsApp Web rejects a patch with two REMOVEs (or two SETs) of the
		// same plaintext index as fatal (WAWebSyncdValidateMutations
		// validateNoSameIndexForMultipleMutations), so emit at most one REMOVE
		// per index. A twin stored under several keys is ambiguous (WhatsApp
		// Web keeps one row per plaintext index): leave it alone.
		if len(foundKeys) > 1 {
			proc.Log.Warnf("PN twin %v of %s is stored under %d app state keys; not removing it", pnIndex, res.lid, len(foundKeys))
			continue
		}
		if len(foundKeys) == 0 {
			continue
		}
		dedupKey := pnIndex[0] + "\x00" + pnIndex[1]
		if seenRemoves[dedupKey] {
			continue
		}
		seenRemoves[dedupKey] = true
		proc.Log.Infof("Adding REMOVE of PN twin %v (key %X) to outgoing %s patch for %s", pnIndex, foundKeys[0], patchInfo.Type, res.lid)
		removes = append(removes, MutationInfo{
			Index:     pnIndex,
			Version:   m.Version,
			Value:     &waSyncAction.SyncActionValue{},
			Operation: waServerSync.SyncdMutation_REMOVE,
			KeyID:     foundKeys[0],
		})
	}
	out.Mutations = append(out.Mutations, removes...)
	out.Mutations = append(out.Mutations, rotationRemoves...)
	if dup := duplicateMutationIndex(out.Mutations); dup != "" {
		proc.Log.Warnf("Expanded %s patch would repeat index %s within one operation; sending it unexpanded", patchInfo.Type, dup)
		return patchInfo, nil
	}
	return out, nil
}

// expandRotationCleanup returns a copy of patchInfo where every SET whose
// index type is in indexTypes, and whose index is stored locally under
// exactly one app state key other than the latest (the one sendAppState
// encodes with), is followed by a REMOVE of that same index under that old
// key, appended after all other mutations in WhatsApp Web's order
// (_generateMutationsToUpload $()). Without it the old-key row stays on the
// server next to the new one, and a receiver in plaintext-index LTHash lookup
// mode subtracts the stored row for the SET while whatsmeow does not,
// diverging the hashes. An index stored under two or more old keys is
// ambiguous and is skipped (logged). The patch is returned unchanged when
// nothing applies; the result never aliases patchInfo.Mutations.
func (proc *Processor) expandRotationCleanup(ctx context.Context, patchInfo PatchInfo, indexTypes []string) (PatchInfo, error) {
	if len(indexTypes) == 0 || proc.Store == nil || proc.Store.AppState == nil || proc.Store.AppStateKeys == nil {
		return patchInfo, nil
	}
	isTarget := func(m MutationInfo) bool {
		if m.Operation != waServerSync.SyncdMutation_SET || len(m.KeyID) != 0 || len(m.Index) == 0 {
			return false
		}
		for _, indexType := range indexTypes {
			if m.Index[0] == indexType {
				return true
			}
		}
		return false
	}
	anyTarget := false
	for _, m := range patchInfo.Mutations {
		if isTarget(m) {
			anyTarget = true
			break
		}
	}
	if !anyTarget {
		return patchInfo, nil
	}
	macsGetter, ok := proc.Store.AppState.(allAppStateMutationMACsGetter)
	if !ok {
		return patchInfo, nil
	}
	keys, err := proc.getAllExpandedKeys(ctx)
	if err != nil {
		return patchInfo, err
	}
	if len(keys) == 0 {
		return patchInfo, nil
	}
	latestKeyID, err := proc.Store.AppStateKeys.GetLatestAppStateSyncKeyID(ctx)
	if err != nil {
		return patchInfo, fmt.Errorf("failed to get latest app state key ID: %w", err)
	}
	macList, err := macsGetter.GetAllAppStateMutationMACs(ctx, string(patchInfo.Type))
	if err != nil {
		return patchInfo, fmt.Errorf("failed to list app state mutation MACs: %w", err)
	}
	macs := make(indexMACSet, len(macList))
	for _, mac := range macList {
		if len(mac.IndexMAC) == 32 {
			macs[*(*[32]byte)(mac.IndexMAC)] = struct{}{}
		}
	}

	var removes []MutationInfo
	seenRemoves := make(map[string]bool)
	for _, m := range patchInfo.Mutations {
		if !isTarget(m) {
			continue
		}
		found, err := findStoredIndexKeys(m.Index, keys, macs)
		if err != nil {
			return patchInfo, err
		}
		var oldKeys [][]byte
		for _, k := range found {
			if !bytes.Equal(k, latestKeyID) {
				oldKeys = append(oldKeys, k)
			}
		}
		if len(oldKeys) > 1 {
			proc.Log.Warnf("Index %v is stored under %d old app state keys; not removing the stale rows", m.Index, len(oldKeys))
			continue
		}
		if len(oldKeys) == 0 {
			continue
		}
		indexBytes, err := json.Marshal(m.Index)
		if err != nil {
			return patchInfo, fmt.Errorf("failed to marshal index: %w", err)
		}
		if seenRemoves[string(indexBytes)] {
			continue
		}
		seenRemoves[string(indexBytes)] = true
		proc.Log.Infof("Adding REMOVE of %s index under rotated-out key %X to outgoing %s patch", m.Index[0], oldKeys[0], patchInfo.Type)
		removes = append(removes, MutationInfo{
			Index:     append([]string(nil), m.Index...),
			Version:   m.Version,
			Value:     &waSyncAction.SyncActionValue{},
			Operation: waServerSync.SyncdMutation_REMOVE,
			KeyID:     oldKeys[0],
		})
	}
	if len(removes) == 0 {
		return patchInfo, nil
	}
	out := patchInfo
	out.Mutations = make([]MutationInfo, 0, len(patchInfo.Mutations)+len(removes))
	out.Mutations = append(out.Mutations, patchInfo.Mutations...)
	out.Mutations = append(out.Mutations, removes...)
	if dup := duplicateMutationIndex(out.Mutations); dup != "" {
		proc.Log.Warnf("Expanded %s patch would repeat index %s within one operation; sending it unexpanded", patchInfo.Type, dup)
		return patchInfo, nil
	}
	return out, nil
}

// duplicateMutationIndex returns the first plaintext index that appears more
// than once among the SET mutations or more than once among the REMOVE
// mutations of a patch, or "" if there is none. WhatsApp Web treats either as
// a fatal error for the whole patch ("same index for multiple mutations in
// patch"), while one SET plus one REMOVE of the same index is allowed.
func duplicateMutationIndex(mutations []MutationInfo) string {
	seen := map[waServerSync.SyncdMutation_SyncdOperation]map[string]bool{}
	for _, m := range mutations {
		indexBytes, err := json.Marshal(m.Index)
		if err != nil {
			return fmt.Sprintf("%v", m.Index)
		}
		key := string(indexBytes)
		if seen[m.Operation] == nil {
			seen[m.Operation] = map[string]bool{}
		}
		if seen[m.Operation][key] {
			return key
		}
		seen[m.Operation][key] = true
	}
	return ""
}
