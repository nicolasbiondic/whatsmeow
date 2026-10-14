// Diagnostic-only helpers to investigate why the primary device does not
// answer app state recovery requests (COMPANION_SYNCD_SNAPSHOT_FATAL_RECOVERY).
// Everything here is a no-op unless WHATSMEOW_APPSTATE_DIAG_DIR is set, and
// nothing here changes how responses are processed.
//
//	WHATSMEOW_APPSTATE_DIAG_DIR=<dir>
//	    Record peer_msg receipts and every recovery response (raw protobuf +
//	    summary) in <dir>.
//
// To send test requests, create <dir>/request-recovery containing a collection
// name (e.g. "regular_low"). The process picks it up within 5 seconds, deletes
// it, and sends one recovery request addressed to the own phone number JID
// and, a minute later, one addressed to the own LID JID (what WhatsApp Web
// does), recording the request IDs so receipts and responses can be matched
// to the variant.
package whatsmeow

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
)

const (
	diagRecoveryDirEnv      = "WHATSMEOW_APPSTATE_DIAG_DIR"
	diagRequestTriggerFile  = "request-recovery"
	diagRequestPollInterval = 5 * time.Second
)

var (
	diagRecoveryFileLock   sync.Mutex
	diagReceiptHandlerOnce sync.Once
	diagRequestPollerOnce  sync.Once
)

func appendRecoveryDiag(record map[string]any) {
	dir := os.Getenv(diagRecoveryDirEnv)
	if dir == "" {
		return
	}
	record["ts"] = time.Now().UTC().Format(time.RFC3339Nano)
	line, err := json.Marshal(record)
	if err != nil {
		return
	}
	diagRecoveryFileLock.Lock()
	defer diagRecoveryFileLock.Unlock()
	if err = os.MkdirAll(dir, 0o700); err != nil {
		return
	}
	f, err := os.OpenFile(filepath.Join(dir, "appstate-diag.jsonl"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return
	}
	defer f.Close()
	_, _ = f.Write(append(line, '\n'))
}

// diagRecordRecoveryResponse dumps a received recovery response before it is
// processed. It only reads.
func (cli *Client) diagRecordRecoveryResponse(
	ctx context.Context,
	reqID types.MessageID,
	result []*waE2E.PeerDataOperationRequestResponseMessage_PeerDataOperationResult,
) {
	dir := os.Getenv(diagRecoveryDirEnv)
	if dir == "" {
		return
	}
	record := map[string]any{"type": "recovery_response", "req_id": string(reqID), "results": len(result)}
	if len(result) > 0 && result[0].GetSyncdSnapshotFatalRecoveryResponse() != nil {
		resp := result[0].GetSyncdSnapshotFatalRecoveryResponse()
		if raw, err := proto.Marshal(resp); err == nil {
			fname := fmt.Sprintf("recovery-%s-%s.pb", reqID, time.Now().UTC().Format("20060102T150405Z"))
			if err = os.WriteFile(filepath.Join(dir, fname), raw, 0o600); err == nil {
				record["file"] = fname
			}
		}
		if snapshot, err := appstate.ParseRecovery(resp); err == nil {
			record["collection"] = snapshot.GetCollectionName()
			record["version"] = snapshot.GetVersion().GetVersion()
			record["records"] = len(snapshot.GetMutationRecords())
			record["lthash"] = hex.EncodeToString(snapshot.GetCollectionLthash())
			if current, _, err := cli.Store.AppState.GetAppStateVersion(ctx, snapshot.GetCollectionName()); err == nil {
				record["local_version"] = current
				record["would_adopt"] = current < snapshot.GetVersion().GetVersion()
			}
		} else {
			record["parse_error"] = err.Error()
		}
	}
	appendRecoveryDiag(record)
}

// diagOnConnect registers the peer receipt recorder and, if requested, sends
// the forced recovery requests. Called in a goroutine after connecting.
func (cli *Client) diagOnConnect() {
	if os.Getenv(diagRecoveryDirEnv) == "" {
		return
	}
	diagReceiptHandlerOnce.Do(func() {
		cli.AddEventHandler(func(evt any) {
			receipt, ok := evt.(*events.Receipt)
			if !ok || receipt.Type != types.ReceiptTypePeerMsg {
				return
			}
			ids := make([]string, len(receipt.MessageIDs))
			for i, id := range receipt.MessageIDs {
				ids[i] = string(id)
			}
			appendRecoveryDiag(map[string]any{
				"type":       "peer_receipt",
				"ids":        ids,
				"from":       receipt.Sender.String(),
				"receipt_ts": receipt.Timestamp.UTC().Format(time.RFC3339),
			})
		})
	})
	diagRequestPollerOnce.Do(func() {
		go cli.diagPollRecoveryRequests(os.Getenv(diagRecoveryDirEnv))
	})
}

func (cli *Client) diagPollRecoveryRequests(dir string) {
	trigger := filepath.Join(dir, diagRequestTriggerFile)
	for {
		time.Sleep(diagRequestPollInterval)
		data, err := os.ReadFile(trigger)
		if err != nil {
			continue
		}
		_ = os.Remove(trigger)
		collection := strings.TrimSpace(string(data))
		if collection == "" {
			collection = string(appstate.WAPatchRegularLow)
		}
		cli.diagSendRecoveryRequests(appstate.WAPatchName(collection))
	}
}

func (cli *Client) diagSendRecoveryRequests(collection appstate.WAPatchName) {
	variants := []struct {
		label string
		to    types.JID
	}{
		{"pn", cli.getOwnID().ToNonAD()},
		{"lid", cli.getOwnLID().ToNonAD()},
	}
	for i, variant := range variants {
		record := map[string]any{
			"type":       "recovery_request_sent",
			"collection": string(collection),
			"variant":    variant.label,
			"to":         variant.to.String(),
		}
		if variant.to.IsEmpty() {
			record["error"] = "own JID unknown"
		} else {
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			resp, err := cli.SendMessage(ctx, variant.to, BuildAppStateRecoveryRequest(collection), SendRequestExtra{Peer: true})
			cancel()
			record["req_id"] = string(resp.ID)
			if err != nil {
				record["error"] = err.Error()
			}
		}
		appendRecoveryDiag(record)
		if i < len(variants)-1 {
			time.Sleep(60 * time.Second)
		}
	}
}
