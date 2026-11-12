package whatsmeow

import (
	"context"
	"errors"
	"testing"

	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/proto/waServerSync"
	"go.mau.fi/whatsmeow/types/events"
	"google.golang.org/protobuf/proto"
)

func TestAppStateUnchangedBaseStillRejectsBadMAC(t *testing.T) {
	for _, kind := range []string{"snapshot", "patch"} {
		t.Run(kind, func(t *testing.T) {
			cli, server := reviewLabelClient(t)
			var patch waServerSync.SyncdPatch
			if err := proto.Unmarshal(server.patches[101], &patch); err != nil {
				t.Fatal(err)
			}
			want := appstate.ErrMismatchingLTHash
			if kind == "snapshot" {
				patch.SnapshotMAC[0] ^= 1
			} else {
				patch.PatchMAC[0] ^= 1
				want = appstate.ErrMismatchingPatchMAC
			}
			raw, err := proto.Marshal(&patch)
			if err != nil {
				t.Fatal(err)
			}
			server.patches[101] = raw
			var reported error
			cli.AddEventHandler(func(evt any) {
				if e, ok := evt.(*events.AppStateSyncError); ok {
					reported = e.Error
				}
			})
			err = cli.FetchAppState(t.Context(), appstate.WAPatchRegular, false, false)
			if !errors.Is(err, want) || !errors.Is(reported, want) || errors.Is(err, appstate.ErrStateChanged) {
				t.Fatalf("unchanged base: result=%v event=%v, want %v", err, reported, want)
			}
			version, hash, err := cli.Store.AppState.GetAppStateVersion(t.Context(), string(appstate.WAPatchRegular))
			if err != nil || version != 100 || hash != sumStoredMACs(t, cli.Store.AppState, appstate.WAPatchRegular) {
				t.Fatalf("bad MAC changed verified state: version=%d err=%v", version, err)
			}
		})
	}
}

func TestAppStateSameVersionReplacementDuringMACReadIsStale(t *testing.T) {
	cli, _ := reviewLabelClient(t)
	original := cli.Store.AppState
	wrapper := &reviewRecoveryBeforeMACStore{AppStateStore: original}
	wrapper.before = func() {
		reviewAdoptLabelRecovery(t, cli)
		_, hash, err := original.GetAppStateVersion(t.Context(), string(appstate.WAPatchRegular))
		if err != nil {
			t.Fatal(err)
		}
		if err := original.PutAppStateVersion(context.Background(), string(appstate.WAPatchRegular), 100, hash); err != nil {
			t.Fatal(err)
		}
	}
	cli.Store.AppState = wrapper
	if err := cli.FetchAppState(t.Context(), appstate.WAPatchRegular, false, false); !errors.Is(err, appstate.ErrStateChanged) {
		t.Fatalf("same-version replacement: %v", err)
	}
}
