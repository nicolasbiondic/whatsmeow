package whatsmeow

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/appstate/lthash"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/proto/waSyncdSnapshotRecovery"
	"go.mau.fi/whatsmeow/types/events"
)

// holdLabelFetch starts a fetch of the "old" label patch and returns once its
// event is held in a handler: one batch is in the handlers, none is queued.
func holdLabelFetch(t *testing.T, ctx context.Context, cli *Client) (release func(), done <-chan error, seen *labelLog) {
	t.Helper()
	paused, resume, fetched := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	var gated atomic.Bool
	cli.AddEventHandler(func(evt any) {
		if label, ok := evt.(*events.LabelEdit); ok && label.Action.GetName() == "old" && !gated.Swap(true) {
			close(paused)
			select {
			case <-resume:
			case <-ctx.Done():
			}
		}
	})
	seen = &labelLog{}
	cli.AddEventHandler(func(evt any) { seen.add(evt) })
	go func() { fetched <- cli.FetchAppState(ctx, appstate.WAPatchRegular, false, false) }()
	select {
	case <-paused:
	case <-ctx.Done():
		t.Fatal("fetch did not reach its label event")
	}
	return func() { close(resume) }, fetched, seen
}

func labelRecovery(t *testing.T, cli *Client, version uint64, label string) []*waE2E.PeerDataOperationRequestResponseMessage_PeerDataOperationResult {
	t.Helper()
	keyID, err := cli.Store.AppStateKeys.GetLatestAppStateSyncKeyID(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	mac := make([]byte, 32)
	mac[0] = byte(version)
	var hash [128]byte
	lthash.WAPatchIntegrity.SubtractThenAddInPlace(hash[:], nil, [][]byte{mac})
	raw, err := proto.Marshal(&waSyncdSnapshotRecovery.SyncdSnapshotRecovery{
		CollectionName: proto.String(string(appstate.WAPatchRegular)), CollectionLthash: hash[:],
		Version: &waSyncdSnapshotRecovery.SyncdVersion{Version: proto.Uint64(version)},
		MutationRecords: []*waSyncdSnapshotRecovery.SyncdPlainTextRecord{{KeyID: keyID, Mac: mac, Value: &waSyncAction.SyncActionData{
			Index: []byte(`["label_edit","7"]`),
			Value: &waSyncAction.SyncActionValue{LabelEditAction: &waSyncAction.LabelEditAction{Name: proto.String(label)}},
		}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	return []*waE2E.PeerDataOperationRequestResponseMessage_PeerDataOperationResult{{
		SyncdSnapshotFatalRecoveryResponse: &waE2E.PeerDataOperationRequestResponseMessage_PeerDataOperationResult_SyncDSnapshotFatalRecoveryResponse{CollectionSnapshot: raw},
	}}
}

// With the backlog of a collection at the limit, new work for it is refused
// before anything is fetched, sent or stored. Nothing waits, nothing that
// was committed is dropped, and what was refused is fetched after the drain.
func TestAppStateBacklogBoundsAdmission(t *testing.T) {
	cli, server := reviewLabelClient(t)
	cli.AppStateBacklogLimit = 3
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	release, fetched, seen := holdLabelFetch(t, ctx, cli)

	reviewAdoptLabelRecovery(t, cli)
	appendLabelPatch(t, cli, server, "newest")
	if err := cli.FetchAppState(ctx, appstate.WAPatchRegular, false, false); err != nil {
		t.Fatal(err)
	}
	if backlog := cli.AppStateBacklog(appstate.WAPatchRegular); backlog.Depth != 3 || backlog.OldestAge <= 0 {
		t.Fatalf("backlog=%+v, want the held batch and two queued behind it", backlog)
	}

	appendLabelPatch(t, cli, server, "later")
	fetches := server.fetches.Load()
	refused := map[string]error{
		"fetch": cli.FetchAppState(ctx, appstate.WAPatchRegular, false, false),
		"captured fetch": func() error {
			_, err := cli.DangerousInternals().FetchAppState(ctx, appstate.WAPatchRegular, false, false)
			return err
		}(),
		"send": cli.SendAppState(ctx, appstate.BuildLabelEdit("7", "local", 1, false)),
	}
	for what, err := range refused {
		var backlog *AppStateBacklogError
		if !errors.Is(err, ErrAppStateBacklog) || !errors.As(err, &backlog) || backlog.Name != appstate.WAPatchRegular || backlog.Depth != 3 {
			t.Fatalf("%s at the limit: %v", what, err)
		}
	}
	if cli.handleAppStateRecovery(ctx, "refused-recovery", labelRecovery(t, cli, 120, "refused")) {
		t.Fatal("a recovery at the limit was reported as handled")
	}
	version, _, err := cli.Store.AppState.GetAppStateVersion(ctx, string(appstate.WAPatchRegular))
	if err != nil || version != 111 || server.fetches.Load() != fetches || server.sends.Load() != 0 {
		t.Fatalf("refused work reached the store or the server: version=%d err=%v fetches=%d sends=%d", version, err, server.fetches.Load()-fetches, server.sends.Load())
	}
	if cli.AppStateBacklog(appstate.WAPatchRegularLow).Depth != 0 || cli.admitAppStateBatch(appstate.WAPatchRegularLow) != nil {
		t.Fatal("another collection was held back")
	}
	if len(seen.get()) != 0 {
		t.Fatalf("delivered %v past the held batch", seen.get())
	}

	release()
	if err := <-fetched; err != nil {
		t.Fatal(err)
	}
	if err := cli.WaitForAppStateEvents(ctx); err != nil {
		t.Fatal(err)
	}
	if backlog := cli.AppStateBacklog(appstate.WAPatchRegular); backlog != (AppStateBacklog{}) {
		t.Fatalf("backlog after the drain: %+v", backlog)
	}
	if err := cli.FetchAppState(ctx, appstate.WAPatchRegular, false, false); err != nil {
		t.Fatalf("fetch after the drain: %v", err)
	}
	if got := seen.get(); !reflect.DeepEqual(got, []string{"old", "new", "newest", "later"}) {
		t.Fatalf("labels delivered as %v", got)
	}
}

// The handler whose own batch fills the backlog gets the error at once.
func TestAppStateBacklogDoesNotBlockAHandler(t *testing.T) {
	cli, server := reviewLabelClient(t)
	cli.AppStateBacklogLimit = 1
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	nested := make(chan error, 1)
	cli.AddEventHandler(func(evt any) {
		if label, ok := evt.(*events.LabelEdit); ok && label.Action.GetName() == "old" {
			appendLabelPatch(t, cli, server, "newer")
			nested <- cli.FetchAppState(ctx, appstate.WAPatchRegular, false, false)
		}
	})
	if err := cli.FetchAppState(ctx, appstate.WAPatchRegular, false, false); err != nil {
		t.Fatal(err)
	}
	if err := <-nested; !errors.Is(err, ErrAppStateBacklog) {
		t.Fatalf("nested fetch at the limit: %v", err)
	}
	if version, _, err := cli.Store.AppState.GetAppStateVersion(ctx, string(appstate.WAPatchRegular)); err != nil || version != 101 {
		t.Fatalf("version=%d err=%v, want the refused patch left on the server", version, err)
	}
}

// Disconnect returns without waiting for committed batches and without
// dropping them. WaitForAppStateEvents is the barrier for shutting down.
func TestDisconnectKeepsDeliveringAppStateEvents(t *testing.T) {
	cli, _ := reviewLabelClient(t)
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	release, fetched, seen := holdLabelFetch(t, ctx, cli)
	reviewAdoptLabelRecovery(t, cli)

	disconnected := make(chan struct{})
	go func() { cli.Disconnect(); close(disconnected) }()
	select {
	case <-disconnected:
	case <-ctx.Done():
		t.Fatal("Disconnect waited for a held event handler")
	}
	if backlog := cli.AppStateBacklog(appstate.WAPatchRegular); backlog.Depth != 2 {
		t.Fatalf("Disconnect changed the backlog: %+v", backlog)
	}
	short, cancelShort := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancelShort()
	if err := cli.WaitForAppStateEvents(short); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("wait while a handler is held: %v", err)
	}

	release()
	if err := <-fetched; err != nil {
		t.Fatal(err)
	}
	if err := cli.WaitForAppStateEvents(ctx); err != nil {
		t.Fatal(err)
	}
	if got := seen.get(); !reflect.DeepEqual(got, []string{"old", "new"}) {
		t.Fatalf("labels delivered after Disconnect: %v", got)
	}
}
