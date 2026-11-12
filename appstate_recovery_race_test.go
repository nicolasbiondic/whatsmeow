package whatsmeow

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/appstate/lthash"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waServerSync"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/proto/waSyncdSnapshotRecovery"
)

type pausedMACReadStore struct {
	*memAppStateStore
	decoded chan struct{}
	resume  chan struct{}
	once    sync.Once
}

func (s *pausedMACReadStore) GetAppStateMutationMAC(ctx context.Context, name string, index []byte) ([]byte, error) {
	mac, err := s.memAppStateStore.GetAppStateMutationMAC(ctx, name, index)
	s.once.Do(func() {
		// The fetch has read v100's MAC for its v101 hash. Let recovery
		// replace the collection before the decoder can persist that hash.
		close(s.decoded)
		select {
		case <-s.resume:
		case <-ctx.Done():
		}
	})
	return mac, err
}

func adoptSyntheticRecovery(t *testing.T, cli *Client, version uint64) {
	t.Helper()
	keyID, err := cli.Store.AppStateKeys.GetLatestAppStateSyncKeyID(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	mac := make([]byte, 32)
	mac[0] = 42
	var hash [128]byte
	lthash.WAPatchIntegrity.SubtractThenAddInPlace(hash[:], nil, [][]byte{mac})
	raw, err := proto.Marshal(&waSyncdSnapshotRecovery.SyncdSnapshotRecovery{
		CollectionName:   proto.String(string(appstate.WAPatchRegularLow)),
		CollectionLthash: hash[:],
		Version:          &waSyncdSnapshotRecovery.SyncdVersion{Version: proto.Uint64(version)},
		MutationRecords: []*waSyncdSnapshotRecovery.SyncdPlainTextRecord{{
			KeyID: keyID, Mac: mac,
			Value: &waSyncAction.SyncActionData{
				Index: []byte(`["archive","100000000000099@lid"]`),
				Value: &waSyncAction.SyncActionValue{ArchiveChatAction: &waSyncAction.ArchiveChatAction{Archived: proto.Bool(true)}},
			},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	result := []*waE2E.PeerDataOperationRequestResponseMessage_PeerDataOperationResult{{
		SyncdSnapshotFatalRecoveryResponse: &waE2E.PeerDataOperationRequestResponseMessage_PeerDataOperationResult_SyncDSnapshotFatalRecoveryResponse{CollectionSnapshot: raw},
	}}
	if !cli.handleAppStateRecovery(t.Context(), "synthetic-race-recovery", result) {
		t.Fatal("recovery handler failed")
	}
}

func TestFetchAppStateDoesNotOverwriteNewerRecovery(t *testing.T) {
	device := newMemAppStateDevice(t)
	env := newAppState409RaceEnv(t, device)
	paused := &pausedMACReadStore{memAppStateStore: device.AppState.(*memAppStateStore), decoded: make(chan struct{}), resume: make(chan struct{})}
	device.AppState = paused
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- env.cli.FetchAppState(ctx, env.name, false, false) }()
	select {
	case <-paused.decoded:
	case <-ctx.Done():
		t.Fatal("fetch did not reach decode")
	}
	adoptSyntheticRecovery(t, env.cli, 110)
	env.checkConsistent(t, 110)
	close(paused.resume)
	select {
	case err := <-done:
		t.Logf("stale fetch result: %v", err)
	case <-ctx.Done():
		t.Fatal("fetch did not finish")
	}
	env.checkConsistent(t, 110)
}

func TestDecodedAppStateDoesNotOverwriteRecovery(t *testing.T) {
	for _, fullSync := range []bool{false, true} {
		for _, recoveryVersion := range []uint64{100, 110} {
			t.Run(fmt.Sprintf("full=%t/recovery=%d", fullSync, recoveryVersion), func(t *testing.T) {
				device := newMemAppStateDevice(t)
				env := newAppState409RaceEnv(t, device)
				var patch waServerSync.SyncdPatch
				if err := proto.Unmarshal(env.server.patches[101], &patch); err != nil {
					t.Fatal(err)
				}
				version, hash, err := device.AppState.GetAppStateVersion(t.Context(), string(env.name))
				if err != nil {
					t.Fatal(err)
				}
				state := appstate.HashState{Version: version, Hash: hash}
				list := &appstate.PatchList{Name: env.name, Patches: []*waServerSync.SyncdPatch{&patch}}
				if fullSync {
					if err := device.AppState.DeleteAppStateVersion(t.Context(), string(env.name)); err != nil {
						t.Fatal(err)
					}
					state = appstate.HashState{}
					list.Patches = nil
					list.Snapshot = &waServerSync.SyncdSnapshot{Version: patch.Version, KeyID: patch.KeyID, Mac: patch.SnapshotMAC}
					for _, mutation := range patch.Mutations {
						list.Snapshot.Records = append(list.Snapshot.Records, mutation.Record)
					}
				}
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				decoded, resume := make(chan struct{}), make(chan struct{})
				env.cli.appStateProc.CollectionLock = func(name appstate.WAPatchName) sync.Locker {
					// Called after all decoding, immediately before the locked
					// version recheck and persistence (snapshot and patch paths).
					close(decoded)
					select {
					case <-resume:
					case <-ctx.Done():
					}
					return env.cli.appStateCollectionLock(name)
				}
				done := make(chan error, 1)
				var batch []any
				go func() {
					_, err := env.cli.applyAppStatePatches(ctx, env.name, state, list, fullSync, &batch)
					done <- err
				}()
				select {
				case <-decoded:
				case <-ctx.Done():
					t.Fatal("decoder did not reach persistence")
				}
				adoptSyntheticRecovery(t, env.cli, recoveryVersion)
				env.checkConsistent(t, recoveryVersion)
				close(resume)
				select {
				case err := <-done:
					if !errors.Is(err, appstate.ErrStateChanged) {
						t.Fatalf("stale decode: %v, want ErrStateChanged", err)
					}
				case <-ctx.Done():
					t.Fatal("decoder did not finish")
				}
				if len(batch) != 0 {
					t.Fatalf("stale mutations dispatched: %v", batch)
				}
				env.checkConsistent(t, recoveryVersion)
			})
		}
	}
}
