package whatsmeow

import (
	"fmt"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waServerSync"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/proto/waSyncdSnapshotRecovery"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

func TestSnapshotCollectorPreservesFullSyncProvenance(t *testing.T) {
	cli := NewClient(&store.Device{}, nil)
	for _, full := range []bool{false, true} {
		var collected []any
		mutations := []appstate.Mutation{
			{Operation: waServerSync.SyncdMutation_SET, Index: []string{appstate.IndexQuickReply, "1"}, Action: &waSyncAction.SyncActionValue{QuickReplyAction: &waSyncAction.QuickReplyAction{Shortcut: proto.String("one")}}},
			{Operation: waServerSync.SyncdMutation_SET, Index: []string{appstate.IndexQuickReply, "2"}, Action: &waSyncAction.SyncActionValue{QuickReplyAction: &waSyncAction.QuickReplyAction{Shortcut: proto.String("two")}}},
		}
		if err := cli.collectEventsToDispatch(t.Context(), appstate.WAPatchRegular, mutations, full, &collected); err != nil {
			t.Fatal(err)
		}
		if len(collected) != 2 {
			t.Fatalf("events = %#v", collected)
		}
		for i, evt := range collected {
			q, ok := evt.(*events.AppState)
			if !ok || q.FromFullSync != full || q.Index[1] != mutations[i].Index[1] {
				t.Fatalf("event %d lost provenance/order: %#v", i, evt)
			}
		}
	}
}

func TestConflictSnapshotHasFullBoundariesAndIncrementalPatches(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		t.Run(fmt.Sprint(corrupt), func(t *testing.T) {
			device := newMemAppStateDevice(t)
			cli := NewClient(device, nil)
			keyID := []byte{1, 2, 3}
			if err := device.AppStateKeys.PutAppStateSyncKey(t.Context(), keyID, store.AppStateSyncKey{Data: make([]byte, 32)}); err != nil {
				t.Fatal(err)
			}
			info := appstate.PatchInfo{Type: appstate.WAPatchRegular, Timestamp: time.Unix(100, 0)}
			for i := range 2 {
				info.Mutations = append(info.Mutations, appstate.MutationInfo{
					Index: []string{appstate.IndexQuickReply, fmt.Sprint(i + 1)}, Version: 2, Operation: waServerSync.SyncdMutation_SET,
					Value: &waSyncAction.SyncActionValue{QuickReplyAction: &waSyncAction.QuickReplyAction{Shortcut: proto.String(fmt.Sprint(i))}},
				})
			}
			encoded, err := cli.appStateProc.EncodePatch(t.Context(), keyID, appstate.HashState{}, info)
			if err != nil {
				t.Fatal(err)
			}
			var patch waServerSync.SyncdPatch
			if err := proto.Unmarshal(encoded, &patch); err != nil {
				t.Fatal(err)
			}
			snapshot := &waServerSync.SyncdSnapshot{Version: &waServerSync.SyncdVersion{Version: proto.Uint64(1)}, KeyID: patch.KeyID, Mac: patch.SnapshotMAC}
			for _, m := range patch.Mutations {
				snapshot.Records = append(snapshot.Records, m.Record)
			}
			if corrupt {
				snapshot.Mac[0] ^= 1
			}
			batch, err := cli.applyAppStateConflicts(t.Context(), info.Type, &appstate.PatchList{Name: info.Type, Snapshot: snapshot})
			if corrupt {
				if err == nil || len(batch) != 0 {
					t.Fatalf("corrupt snapshot dispatched: %#v %v", batch, err)
				}
				return
			}
			if err != nil || len(batch) != 4 {
				t.Fatalf("snapshot batch = %#v %v", batch, err)
			}
			if start, ok := batch[0].(*events.AppStateSyncStart); !ok || start.Name != info.Type || start.Recovery {
				t.Fatalf("missing opening: %#v", batch[0])
			}
			for i := range 2 {
				q, ok := batch[i+1].(*events.AppState)
				if !ok || !q.FromFullSync || q.Index[1] != fmt.Sprint(i+1) {
					t.Fatalf("snapshot provenance/order = %#v", batch[i+1])
				}
			}
			if end, ok := batch[3].(*events.AppStateSyncComplete); !ok || end.Name != info.Type || end.Version != 1 || end.Failed {
				t.Fatalf("missing close: %#v", batch[3])
			}
			state := appstate.HashState{Version: 1, Hash: sumStoredMACs(t, device.AppState, info.Type)}
			info.Mutations = info.Mutations[:1]
			info.Mutations[0].Value.QuickReplyAction.Shortcut = proto.String("delta")
			encoded, err = cli.appStateProc.EncodePatch(t.Context(), keyID, state, info)
			if err != nil {
				t.Fatal(err)
			}
			var delta waServerSync.SyncdPatch
			if err := proto.Unmarshal(encoded, &delta); err != nil {
				t.Fatal(err)
			}
			delta.Version = &waServerSync.SyncdVersion{Version: proto.Uint64(2)}
			// Repeat from an empty client to cover a snapshot PLUS following patch
			// in the same conflict response. Only snapshot records are full sync.
			if err := device.AppState.DeleteAppStateVersion(t.Context(), string(info.Type)); err != nil {
				t.Fatal(err)
			}
			batch, err = cli.applyAppStateConflicts(t.Context(), info.Type, &appstate.PatchList{Name: info.Type, Snapshot: snapshot, Patches: []*waServerSync.SyncdPatch{&delta}})
			if err != nil || len(batch) != 5 {
				t.Fatalf("snapshot + patch batch = %#v %v", batch, err)
			}
			if q, ok := batch[4].(*events.AppState); !ok || q.FromFullSync {
				t.Fatalf("delta acquired full provenance: %#v", batch[4])
			}
		})
	}
}

func TestRejectedRecoveryDispatchBalancesOnlyDeliveredOpenings(t *testing.T) {
	for _, rejectAt := range []string{"start", "mutation", "complete", "panic"} {
		for rejectHandler := range 3 {
			t.Run(fmt.Sprintf("%s/%d", rejectAt, rejectHandler), func(t *testing.T) {
				cli := NewClient(&store.Device{}, nil)
				var opened, closed, failed [3]int
				reject := true
				for i := range 3 {
					cli.AddEventHandlerWithSuccessStatus(func(evt any) bool {
						stage := "mutation"
						switch v := evt.(type) {
						case *events.AppStateSyncStart:
							opened[i]++
							stage = "start"
						case *events.AppStateSyncComplete:
							stage = "complete"
							if v.Failed {
								failed[i]++
								closed[i]++
								return false
							}
						}
						if reject && i == rejectHandler && (stage == rejectAt || rejectAt == "panic" && stage == "mutation") {
							if rejectAt == "panic" {
								panic("injected")
							}
							return false
						}
						if stage == "complete" {
							closed[i]++
						}
						return true
					})
				}
				batch := []any{&events.AppStateSyncStart{Name: appstate.WAPatchRegular, Recovery: true}, &events.AppState{}, &events.AppStateSyncComplete{Name: appstate.WAPatchRegular, Recovery: true}}
				if !cli.dispatchAppStateRecovery(batch) {
					t.Fatal("rejection ignored")
				}
				for i := range 3 {
					if opened[i] != closed[i] {
						t.Fatalf("handler %d: starts=%v closes=%v", i, opened, closed)
					}
					want := 1
					if rejectAt == "start" && i > rejectHandler || rejectAt == "complete" && i < rejectHandler {
						want = 0
					}
					if failed[i] != want {
						t.Fatalf("handler %d failed closes=%v", i, failed)
					}
				}
				reject = false
				if cli.dispatchAppStateRecovery(batch) {
					t.Fatal("retry rejected")
				}
				for i := range 3 {
					if opened[i] != closed[i] {
						t.Fatalf("retry unbalanced: %v/%v", opened, closed)
					}
				}
			})
		}
	}
}

func TestRecoverySnapshotStartsBeforeCompletion(t *testing.T) {
	for _, emitMutations := range []bool{false, true} {
		cli := NewClient(&store.Device{AppState: newFakeAppStateVersionStore()}, nil)
		cli.EmitAppStateEventsOnFullSync = emitMutations
		snapshot, err := proto.Marshal(&waSyncdSnapshotRecovery.SyncdSnapshotRecovery{
			CollectionName:   proto.String(string(appstate.WAPatchRegular)),
			CollectionLthash: make([]byte, 128),
			Version:          &waSyncdSnapshotRecovery.SyncdVersion{Version: proto.Uint64(1)},
		})
		if err != nil {
			t.Fatal(err)
		}
		var collected []any
		cli.AddEventHandler(func(evt any) { collected = append(collected, evt) })
		result := []*waE2E.PeerDataOperationRequestResponseMessage_PeerDataOperationResult{{
			SyncdSnapshotFatalRecoveryResponse: &waE2E.PeerDataOperationRequestResponseMessage_PeerDataOperationResult_SyncDSnapshotFatalRecoveryResponse{CollectionSnapshot: snapshot},
		}}
		if !cli.handleAppStateRecovery(t.Context(), "fixture", result) {
			t.Fatal("recovery handler failed")
		}
		if len(collected) != 2 {
			t.Fatalf("recovery events = %#v", collected)
		}
		start, ok := collected[0].(*events.AppStateSyncStart)
		if !ok || start.Name != appstate.WAPatchRegular || !start.Recovery {
			t.Fatalf("missing start: %#v", collected[0])
		}
		end, ok := collected[1].(*events.AppStateSyncComplete)
		if !ok || end.Name != start.Name || !end.Recovery {
			t.Fatalf("missing close: %#v", collected[1])
		}
	}
}
