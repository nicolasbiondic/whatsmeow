package whatsmeow

import (
	"testing"

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
