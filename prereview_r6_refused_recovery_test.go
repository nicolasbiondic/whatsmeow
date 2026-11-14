// Copy: sdk; package: whatsmeow. Real protocol-message/recovery/refusal paths,
// synthetic Noise peer and SQLite; input enters immediately after decryption.
package whatsmeow

import (
	"context"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

func TestPrereviewR6RefusedRecoveryKeepsMessageUnacknowledged(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	cli, _ := reviewLabelClient(t)
	cli.AppStateBacklogLimit = 1
	notices := watchBacklog(cli)
	release, fetched, _ := holdLabelFetch(t, ctx, cli)
	// Feed the actual peer-response protocol path, not just the recovery
	// helper's bool. The latter's false is supposed to prevent acknowledgement.
	info := &types.MessageInfo{MessageSource: types.MessageSource{
		IsFromMe: true, Sender: cli.Store.ID.ToNonAD(), Chat: cli.Store.ID.ToNonAD(),
	}, ID: "synthetic-recovery"}
	msg := &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{
		PeerDataOperationRequestResponseMessage: &waE2E.PeerDataOperationRequestResponseMessage{
			PeerDataOperationRequestType: waE2E.PeerDataOperationRequestType_COMPANION_SYNCD_SNAPSHOT_FATAL_RECOVERY.Enum(),
			StanzaID:                     proto.String("synthetic-request"),
			PeerDataOperationResult:      labelRecovery(t, cli, 110, "recovered"),
		},
	}}
	handlerFailed := cli.handleDecryptedMessage(ctx, info, msg, 0)
	before := labelVersion(t, cli)
	release()
	if err := <-fetched; err != nil {
		t.Fatal(err)
	}
	if err := cli.WaitForAppStateEvents(ctx); err != nil {
		t.Fatal(err)
	}
	after := labelVersion(t, cli)
	_, owed := cli.appStatePublisher.debt(appstate.WAPatchRegular)
	t.Logf("refused_snapshot=110 cursor_before=%d cursor_after=%d message_handler_failed=%t caught_up_notices=%d debt_remaining=%t",
		before, after, handlerFailed, len(notices.caughtUp()), owed)
	if !handlerFailed && before == 101 && after == 101 && len(notices.caughtUp()) == 1 && !owed {
		t.Fatal("refused recovery becomes message success and an empty incremental fetch declares it caught up")
	}
}
