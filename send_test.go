// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"testing"

	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
)

func peerDataOperationRequest(reqType waE2E.PeerDataOperationRequestType) *waE2E.Message {
	return &waE2E.Message{
		ProtocolMessage: &waE2E.ProtocolMessage{
			Type: waE2E.ProtocolMessage_PEER_DATA_OPERATION_REQUEST_MESSAGE.Enum(),
			PeerDataOperationRequestMessage: &waE2E.PeerDataOperationRequestMessage{
				PeerDataOperationRequestType: reqType.Enum(),
			},
		},
	}
}

func TestPeerMessageAttributes(t *testing.T) {
	to := types.JID{User: "15550001234", Server: types.DefaultUserServer}
	for _, tc := range []struct {
		name                 string
		message              *waE2E.Message
		wantPushPriority     any
		wantPrivacySensitive any
	}{{
		name: "AppStateKeyRequest",
		message: &waE2E.Message{ProtocolMessage: &waE2E.ProtocolMessage{
			Type: waE2E.ProtocolMessage_APP_STATE_SYNC_KEY_REQUEST.Enum(),
		}},
		wantPushPriority: "high",
	}, {
		name:                 "HistorySyncOnDemand",
		message:              peerDataOperationRequest(waE2E.PeerDataOperationRequestType_HISTORY_SYNC_ON_DEMAND),
		wantPushPriority:     "high_force",
		wantPrivacySensitive: "1",
	}, {
		// The primary only answers fatal recovery requests while its app is
		// awake, so the request must wake it like WhatsApp Web's does.
		name:             "SyncdSnapshotFatalRecovery",
		message:          peerDataOperationRequest(waE2E.PeerDataOperationRequestType_COMPANION_SYNCD_SNAPSHOT_FATAL_RECOVERY),
		wantPushPriority: "high",
	}, {
		name:    "OtherPeerMessage",
		message: peerDataOperationRequest(waE2E.PeerDataOperationRequestType_PLACEHOLDER_MESSAGE_RESEND),
	}} {
		t.Run(tc.name, func(t *testing.T) {
			attrs := peerMessageAttributes(to, "3EB0TESTID", tc.message)
			if attrs["id"] != types.MessageID("3EB0TESTID") || attrs["type"] != "text" || attrs["category"] != "peer" || attrs["to"] != to {
				t.Errorf("unexpected base attributes: %v", attrs)
			}
			if got := attrs["push_priority"]; got != tc.wantPushPriority {
				t.Errorf("push_priority = %v, want %v", got, tc.wantPushPriority)
			}
			if got := attrs["privacy_sensitive"]; got != tc.wantPrivacySensitive {
				t.Errorf("privacy_sensitive = %v, want %v", got, tc.wantPrivacySensitive)
			}
		})
	}
}
