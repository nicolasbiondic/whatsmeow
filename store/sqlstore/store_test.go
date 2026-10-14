// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package sqlstore

import (
	"context"
	"encoding/hex"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"go.mau.fi/whatsmeow/proto/waAdv"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
)

func newTestAppStateStore(t *testing.T) *SQLStore {
	t.Helper()
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "test.db")
	container, err := New(ctx, "sqlite3", "file:"+dbPath+"?_foreign_keys=on", nil)
	if err != nil {
		t.Fatalf("open container: %v", err)
	}
	t.Cleanup(func() { container.Close() })
	device := container.NewDevice()
	device.ID = &types.JID{User: "15550001234", Device: 1, Server: types.DefaultUserServer}
	device.Account = &waAdv.ADVSignedDeviceIdentity{
		Details:             []byte{},
		AccountSignature:    make([]byte, 64),
		AccountSignatureKey: make([]byte, 32),
		DeviceSignature:     make([]byte, 64),
	}
	if err = container.PutDevice(ctx, device); err != nil {
		t.Fatalf("put device: %v", err)
	}
	s, ok := device.AppState.(*SQLStore)
	if !ok {
		t.Fatalf("device.AppState is %T, want *SQLStore", device.AppState)
	}
	return s
}

func testMAC(b byte) []byte {
	out := make([]byte, 32)
	for i := range out {
		out[i] = b
	}
	return out
}

// GetAllAppStateMutationMACs must return exactly one row per index MAC: the
// value stored at its highest version, i.e. the same value that
// GetAppStateMutationMAC (ORDER BY version DESC LIMIT 1) returns. Exercised
// with index MACs that are stored at several versions.
func TestGetAllAppStateMutationMACsLatestPerIndex(t *testing.T) {
	ctx := context.Background()
	s := newTestAppStateStore(t)
	const name = "regular_low"

	if err := s.PutAppStateVersion(ctx, name, 3, [128]byte{}); err != nil {
		t.Fatalf("put version: %v", err)
	}
	// v1: A→a1, B→b1; v2: B→b2, C→c2; v3: C→c3, D→d3.
	// B and C exist at two versions each; only b2, c3 may be returned.
	put := func(version uint64, macs ...store.AppStateMutationMAC) {
		t.Helper()
		if err := s.PutAppStateMutationMACs(ctx, name, version, macs); err != nil {
			t.Fatalf("put MACs v%d: %v", version, err)
		}
	}
	idxA, idxB, idxC, idxD := testMAC(0xA0), testMAC(0xB0), testMAC(0xC0), testMAC(0xD0)
	put(1,
		store.AppStateMutationMAC{IndexMAC: idxA, ValueMAC: testMAC(0xA1)},
		store.AppStateMutationMAC{IndexMAC: idxB, ValueMAC: testMAC(0xB1)},
	)
	put(2,
		store.AppStateMutationMAC{IndexMAC: idxB, ValueMAC: testMAC(0xB2)},
		store.AppStateMutationMAC{IndexMAC: idxC, ValueMAC: testMAC(0xC2)},
	)
	put(3,
		store.AppStateMutationMAC{IndexMAC: idxC, ValueMAC: testMAC(0xC3)},
		store.AppStateMutationMAC{IndexMAC: idxD, ValueMAC: testMAC(0xD3)},
	)

	all, err := s.GetAllAppStateMutationMACs(ctx, name)
	if err != nil {
		t.Fatalf("get all: %v", err)
	}
	want := map[string]string{
		hex.EncodeToString(idxA): hex.EncodeToString(testMAC(0xA1)),
		hex.EncodeToString(idxB): hex.EncodeToString(testMAC(0xB2)),
		hex.EncodeToString(idxC): hex.EncodeToString(testMAC(0xC3)),
		hex.EncodeToString(idxD): hex.EncodeToString(testMAC(0xD3)),
	}
	if len(all) != len(want) {
		t.Fatalf("got %d rows, want %d (one per distinct index MAC): %v", len(all), len(want), all)
	}
	got := make(map[string]string, len(all))
	for _, m := range all {
		idx := hex.EncodeToString(m.IndexMAC)
		if _, dup := got[idx]; dup {
			t.Fatalf("index MAC %s returned more than once", idx)
		}
		got[idx] = hex.EncodeToString(m.ValueMAC)
	}
	for idx, val := range want {
		if got[idx] != val {
			t.Errorf("index %s: got value %s, want %s", idx, got[idx], val)
		}
		// Must match the single-row lookup that DecodePatches/EncodePatch use.
		raw, _ := hex.DecodeString(idx)
		single, err := s.GetAppStateMutationMAC(ctx, name, raw)
		if err != nil {
			t.Fatalf("single lookup: %v", err)
		}
		if hex.EncodeToString(single) != val {
			t.Errorf("index %s: GetAppStateMutationMAC = %x, want %s", idx, single, val)
		}
	}

	// Unknown collection: no rows, no error.
	empty, err := s.GetAllAppStateMutationMACs(ctx, "regular_high")
	if err != nil || len(empty) != 0 {
		t.Fatalf("empty collection: got %d rows, err %v", len(empty), err)
	}
}

// Guard the query's cost: a correlated MAX(version) subquery takes over a
// minute on a collection with a few tens of thousands of rows, the set-based
// window query stays in the millisecond range.
func TestGetAllAppStateMutationMACsScalesLinearly(t *testing.T) {
	ctx := context.Background()
	s := newTestAppStateStore(t)
	const name = "regular"
	const rows = 20000
	if err := s.PutAppStateVersion(ctx, name, 2, [128]byte{}); err != nil {
		t.Fatalf("put version: %v", err)
	}
	batch := make([]store.AppStateMutationMAC, 0, rows)
	for i := 0; i < rows; i++ {
		index := make([]byte, 32)
		index[0], index[1], index[2] = byte(i>>16), byte(i>>8), byte(i)
		batch = append(batch, store.AppStateMutationMAC{IndexMAC: index, ValueMAC: testMAC(byte(i))})
	}
	if err := s.PutAppStateMutationMACs(ctx, name, 1, batch); err != nil {
		t.Fatalf("put MACs: %v", err)
	}
	start := time.Now()
	got, err := s.GetAllAppStateMutationMACs(ctx, name)
	if err != nil {
		t.Fatalf("GetAllAppStateMutationMACs: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("listing %d rows took %v", rows, elapsed)
	}
	if len(got) != rows {
		t.Fatalf("got %d rows, want %d", len(got), rows)
	}
}
