package whatsmeow

import (
	"context"
	"errors"
	"testing"

	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
)

// failLabelReset runs a full sync that is cancelled right after its reset
// and whose restore fails too: the live collection is empty and the v100
// state only survives in the durable reset journal.
func failLabelReset(t *testing.T, cli *Client) *sqlstore.SQLStore {
	t.Helper()
	original := cli.Store.AppState.(*sqlstore.SQLStore)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	injected := errors.New("injected restore unavailable")
	cli.Store.AppState = &failingAtomicRestoreStore{
		cancelAfterResetStore: &cancelAfterResetStore{AppStateStore: original, cancel: cancel},
		failure:               injected,
	}
	err := cli.FetchAppState(ctx, appstate.WAPatchRegular, true, false)
	cli.Store.AppState = original
	if !errors.Is(err, context.Canceled) || !errors.Is(err, injected) {
		t.Fatalf("full sync did not fail with a failed restore: %v", err)
	}
	if version, _, err := original.GetAppStateVersion(t.Context(), "regular"); err != nil || version != 0 {
		t.Fatalf("live state after the failed restore: version=%d err=%v", version, err)
	}
	return original
}

func reopenedLabelVersion(t *testing.T, original *sqlstore.SQLStore) uint64 {
	t.Helper()
	jid, _ := types.ParseJID(original.JID)
	reopened := sqlstore.NewSQLStore(original.Container, jid)
	version, hash, err := reopened.GetAppStateVersion(t.Context(), "regular")
	if err != nil || hash != sumStoredMACs(t, reopened, appstate.WAPatchRegular) {
		t.Fatalf("reopened store is not consistent: version=%d err=%v", version, err)
	}
	return version
}

// The store instance that failed to restore must not build on the emptied
// collection: the next fetch first puts the journaled state back, so an
// incremental fetch continues from it instead of replaying from zero.
func TestFailedResetIsSettledBeforeTheNextFetch(t *testing.T) {
	cli, server := reviewLabelClient(t)
	original := failLabelReset(t, cli)
	fetchesBefore := server.fetches.Load()
	if err := cli.FetchAppState(t.Context(), appstate.WAPatchRegular, false, false); err != nil {
		t.Fatal(err)
	}
	version, hash, err := original.GetAppStateVersion(t.Context(), "regular")
	if err != nil || version != 101 || hash != sumStoredMACs(t, original, appstate.WAPatchRegular) {
		t.Fatalf("fetch did not continue from the journaled state: version=%d err=%v", version, err)
	}
	if server.fetches.Load() != fetchesBefore+1 {
		t.Fatalf("expected one incremental fetch, got %d", server.fetches.Load()-fetchesBefore)
	}
	if reopened := reopenedLabelVersion(t, original); reopened != 101 {
		t.Fatalf("reopened store went back to v%d", reopened)
	}
}

// unrecoveredResetStore stands for a store that cannot put a leftover journal
// back on request, so the next attempt sees an empty collection.
type unrecoveredResetStore struct {
	store.AppStateStore
}

func (s *unrecoveredResetStore) RecoverAppStateBackup(context.Context, string) error { return nil }

func (s *unrecoveredResetStore) GetAllAppStateMutationMACs(ctx context.Context, name string) ([]store.AppStateMutationMAC, error) {
	return s.AppStateStore.(appStateMutationMACDumper).GetAllAppStateMutationMACs(ctx, name)
}

// Even when an attempt cannot see the leftover journal, because its capture
// found version 0, a replay that completes retires it: the lower cursor it
// established must survive a reopen.
func TestCompletedReplayRetiresAJournalItCouldNotSee(t *testing.T) {
	for _, tc := range []struct {
		name     string
		fullSync bool
	}{{"full_sync", true}, {"fetch_from_zero", false}} {
		t.Run(tc.name, func(t *testing.T) {
			cli, server := reviewLabelClient(t)
			original := failLabelReset(t, cli)
			keyID, err := cli.Store.AppStateKeys.GetLatestAppStateSyncKeyID(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			raw, err := cli.appStateProc.EncodePatch(t.Context(), keyID, appstate.HashState{}, appstate.BuildLabelEdit("7", "retry", 1, false))
			if err != nil {
				t.Fatal(err)
			}
			server.mu.Lock()
			server.version = 0
			server.patches = make(map[uint64][]byte)
			server.appendPatch(raw)
			server.reviewAllowSnapshot = true
			server.mu.Unlock()
			cli.Store.AppState = &unrecoveredResetStore{AppStateStore: original}
			if err := cli.FetchAppState(t.Context(), appstate.WAPatchRegular, tc.fullSync, false); err != nil {
				t.Fatal(err)
			}
			if reopened := reopenedLabelVersion(t, original); reopened != 1 {
				t.Fatalf("stale reset journal undid the completed replay: reopened at v%d", reopened)
			}
		})
	}
}
