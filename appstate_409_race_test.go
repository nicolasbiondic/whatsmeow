// Copyright (c) 2026 Tulir Asokan
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package whatsmeow

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"
	"golang.org/x/crypto/hkdf"
	"google.golang.org/protobuf/proto"

	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/appstate/lthash"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/proto/waServerSync"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/socket"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// This file reproduces the race between SendAppState's 409 conflict handling
// and a notification-triggered FetchAppState for the same collection.
//
// Scenario (seen in production, twice on one line in 8 hours):
//
//  1. The client sends a patch encoded against local version N.
//  2. Another device had already written version N+1, so the server answers
//     409 and includes patch N+1 in the response.
//  3. In parallel, the server's "app state updated" notification makes the
//     client run FetchAppState, which (under appStateSyncLock) downloads and
//     applies the same patch N+1.
//  4. The 409 handler applies the response's copy of N+1 as well, on top of
//     the version-N state it read before sending and without the lock.
//
// Applying one patch twice is not idempotent for the LTHash: the second
// application looks up the "previous value MAC" of each SET in the database
// and finds the value MAC the first application just stored, so it adds and
// subtracts the same value and never removes the old one. With strict
// verification this surfaces as a spurious ErrMismatchingLTHash, or as a
// UNIQUE violation on the MAC table (whose version/hash row was already
// written by then). A client that tolerates mismatching patches the way
// WhatsApp Web does persists the wrong hash.
//
// The test drives the real SendAppState/FetchAppState code through the real
// request path (sendIQ over a NoiseSocket) against a tiny in-process syncd
// server, so it covers the lock discipline of both paths, not just the
// patch decoder.

// memAppStateStore is an in-memory store.AppStateStore with the same
// constraints as the SQL schema: a (name, version, index_mac) primary key on
// the MAC table and latest-version-wins lookups.
type memAppStateStore struct {
	mu       sync.Mutex
	versions map[string]uint64
	hashes   map[string][128]byte
	// macs[name][indexMAC] = rows at each version
	macs map[string]map[[32]byte]map[uint64][]byte
	keys map[string]*store.AppStateSyncKey
	// newest key first
	keyOrder [][]byte
}

var _ store.AppStateStore = (*memAppStateStore)(nil)
var _ store.AppStateSyncKeyStore = (*memAppStateStore)(nil)

func newMemAppStateStore() *memAppStateStore {
	return &memAppStateStore{
		versions: make(map[string]uint64),
		hashes:   make(map[string][128]byte),
		macs:     make(map[string]map[[32]byte]map[uint64][]byte),
		keys:     make(map[string]*store.AppStateSyncKey),
	}
}

func (m *memAppStateStore) PutAppStateVersion(_ context.Context, name string, version uint64, hash [128]byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.versions[name] = version
	m.hashes[name] = hash
	return nil
}

func (m *memAppStateStore) GetAppStateVersion(_ context.Context, name string) (uint64, [128]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.versions[name], m.hashes[name], nil
}

func (m *memAppStateStore) DeleteAppStateVersion(_ context.Context, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.versions, name)
	delete(m.hashes, name)
	delete(m.macs, name)
	return nil
}

func (m *memAppStateStore) PutAppStateMutationMACs(_ context.Context, name string, version uint64, mutations []store.AppStateMutationMAC) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.macs[name] == nil {
		m.macs[name] = make(map[[32]byte]map[uint64][]byte)
	}
	// Check the whole batch first: the SQL implementation inserts in one
	// transaction, so a duplicate rejects everything.
	for _, mut := range mutations {
		if _, exists := m.macs[name][[32]byte(mut.IndexMAC)][version]; exists {
			return fmt.Errorf("UNIQUE constraint failed: whatsmeow_app_state_mutation_macs.name, version, index_mac (v%d, %x)", version, mut.IndexMAC[:4])
		}
	}
	for _, mut := range mutations {
		idx := [32]byte(mut.IndexMAC)
		if m.macs[name][idx] == nil {
			m.macs[name][idx] = make(map[uint64][]byte)
		}
		m.macs[name][idx][version] = bytes.Clone(mut.ValueMAC)
	}
	return nil
}

func (m *memAppStateStore) DeleteAppStateMutationMACs(_ context.Context, name string, indexMACs [][]byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, idx := range indexMACs {
		delete(m.macs[name], [32]byte(idx))
	}
	return nil
}

func (m *memAppStateStore) latestLocked(name string, idx [32]byte) []byte {
	var best uint64
	var val []byte
	for version, v := range m.macs[name][idx] {
		if val == nil || version > best {
			best, val = version, v
		}
	}
	return val
}

func (m *memAppStateStore) GetAppStateMutationMAC(_ context.Context, name string, indexMAC []byte) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.latestLocked(name, [32]byte(indexMAC)), nil
}

func (m *memAppStateStore) GetAllAppStateMutationMACs(_ context.Context, name string) ([]store.AppStateMutationMAC, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []store.AppStateMutationMAC
	for idx := range m.macs[name] {
		out = append(out, store.AppStateMutationMAC{IndexMAC: idx[:], ValueMAC: m.latestLocked(name, idx)})
	}
	return out, nil
}

func (m *memAppStateStore) PutAppStateSyncKey(_ context.Context, id []byte, key store.AppStateSyncKey) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.keys[string(id)] = &key
	m.keyOrder = append([][]byte{bytes.Clone(id)}, m.keyOrder...)
	return nil
}

func (m *memAppStateStore) GetAppStateSyncKey(_ context.Context, id []byte) (*store.AppStateSyncKey, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.keys[string(id)], nil
}

func (m *memAppStateStore) GetLatestAppStateSyncKeyID(_ context.Context) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.keyOrder) == 0 {
		return nil, nil
	}
	return m.keyOrder[0], nil
}

func (m *memAppStateStore) GetAllAppStateSyncKeys(_ context.Context) ([]*store.AppStateSyncKey, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]*store.AppStateSyncKey, 0, len(m.keyOrder))
	for _, id := range m.keyOrder {
		out = append(out, m.keys[string(id)])
	}
	return out, nil
}

// newMemAppStateDevice returns a device whose app state tables live in memory.
func newMemAppStateDevice(t *testing.T) *store.Device {
	t.Helper()
	mem := newMemAppStateStore()
	return &store.Device{
		ID:           &types.JID{User: "15551234567", Device: 1, Server: types.DefaultUserServer},
		AppState:     mem,
		AppStateKeys: mem,
		ChatSettings: &store.NoopStore{},
	}
}

type appStateMACLister interface {
	GetAllAppStateMutationMACs(ctx context.Context, name string) ([]store.AppStateMutationMAC, error)
}

// sumStoredMACs recomputes the collection's LTHash from the stored latest
// value MAC of every index, i.e. what the server's state should hash to.
func sumStoredMACs(t *testing.T, appState store.AppStateStore, name appstate.WAPatchName) [128]byte {
	t.Helper()
	lister, ok := appState.(appStateMACLister)
	if !ok {
		t.Fatalf("%T cannot list its mutation MACs", appState)
	}
	macs, err := lister.GetAllAppStateMutationMACs(context.Background(), string(name))
	if err != nil {
		t.Fatalf("list MACs: %v", err)
	}
	values := make([][]byte, len(macs))
	for i, m := range macs {
		values[i] = m.ValueMAC
	}
	var sum [128]byte
	lthash.WAPatchIntegrity.SubtractThenAddInPlace(sum[:], nil, values)
	return sum
}

// fakeSyncdServer is the minimum of WhatsApp's w:sync:app:state server needed
// for SendAppState and FetchAppState: one collection, a patch log, 409 on
// version mismatch (with the missing patches in the response, like the real
// server), and no snapshots.
type fakeSyncdServer struct {
	t    *testing.T
	cli  *Client
	name appstate.WAPatchName

	mu      sync.Mutex
	version uint64
	patches map[uint64][]byte // marshaled SyncdPatch with Version set

	sends, conflicts, fetches atomic.Int32

	// beforeConflict, when set, runs before a 409 response is delivered.
	// Tests use it to order the race deterministically.
	beforeConflict func()
}

func (s *fakeSyncdServer) appendPatch(raw []byte) uint64 {
	var patch waServerSync.SyncdPatch
	if err := proto.Unmarshal(raw, &patch); err != nil {
		s.t.Fatalf("server: unmarshal patch: %v", err)
	}
	s.version++
	patch.Version = &waServerSync.SyncdVersion{Version: proto.Uint64(s.version)}
	stored, err := proto.Marshal(&patch)
	if err != nil {
		s.t.Fatalf("server: marshal patch: %v", err)
	}
	s.patches[s.version] = stored
	return s.version
}

func (s *fakeSyncdServer) patchNodesSince(version uint64) []waBinary.Node {
	var nodes []waBinary.Node
	for v := version + 1; v <= s.version; v++ {
		nodes = append(nodes, waBinary.Node{Tag: "patch", Content: s.patches[v]})
	}
	return nodes
}

func (s *fakeSyncdServer) handleIQ(ctx context.Context, iq *waBinary.Node) {
	id, _ := iq.Attrs["id"].(string)
	if iq.Attrs["xmlns"] != "w:sync:app:state" {
		s.t.Logf("server: ignoring IQ %s", iq)
		return
	}
	collection, ok := iq.GetOptionalChildByTag("sync", "collection")
	if !ok {
		s.t.Errorf("server: IQ without sync/collection: %s", iq)
		return
	}
	ag := collection.AttrGetter()
	name := ag.String("name")
	version := ag.Uint64("version")
	var resp waBinary.Node
	if patchNode, isSend := collection.GetOptionalChildByTag("patch"); isSend {
		s.sends.Add(1)
		raw, _ := patchNode.Content.([]byte)
		s.mu.Lock()
		if version != s.version {
			s.conflicts.Add(1)
			pending := s.patchNodesSince(version)
			s.mu.Unlock()
			if s.beforeConflict != nil {
				s.beforeConflict()
			}
			resp = waBinary.Node{
				Tag:   "collection",
				Attrs: waBinary.Attrs{"name": name, "type": "error"},
				Content: []waBinary.Node{
					{Tag: "error", Attrs: waBinary.Attrs{"code": "409", "text": "conflict"}},
					{Tag: "patches", Content: pending},
				},
			}
		} else {
			newVersion := s.appendPatch(raw)
			s.mu.Unlock()
			resp = waBinary.Node{
				Tag:   "collection",
				Attrs: waBinary.Attrs{"name": name, "version": fmt.Sprint(newVersion)},
			}
		}
	} else {
		s.fetches.Add(1)
		if ag.OptionalBool("return_snapshot") {
			s.t.Errorf("server: unexpected snapshot request for %s", name)
		}
		s.mu.Lock()
		pending := s.patchNodesSince(version)
		s.mu.Unlock()
		resp = waBinary.Node{
			Tag:   "collection",
			Attrs: waBinary.Attrs{"name": name, "has_more_patches": "false"},
			Content: []waBinary.Node{
				{Tag: "patches", Content: pending},
			},
		}
	}
	s.cli.receiveResponse(ctx, &waBinary.Node{
		Tag:     "iq",
		Attrs:   waBinary.Attrs{"id": id, "type": "result", "from": types.ServerJID},
		Content: []waBinary.Node{{Tag: "sync", Content: []waBinary.Node{resp}}},
	})
}

func newGCM(t *testing.T, key []byte) cipher.AEAD {
	t.Helper()
	block, err := aes.NewCipher(key)
	if err != nil {
		t.Fatal(err)
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		t.Fatal(err)
	}
	return aead
}

// attachFakeServer connects cli to an in-process websocket server and installs
// a NoiseSocket on it, so cli.sendIQ works without a real handshake. Both ends
// derive the same keys the way NoiseHandshake.Finish does from a handshake
// that mixed in nothing (HKDF over the start pattern only).
func attachFakeServer(t *testing.T, cli *Client, name appstate.WAPatchName) *fakeSyncdServer {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	salt := []byte(socket.NoiseStartPattern)
	if len(salt) != 32 {
		sum := sha256.Sum256(salt)
		salt = sum[:]
	}
	kdf := hkdf.New(sha256.New, nil, salt, nil)
	clientWriteKey := make([]byte, 32)
	if _, err := io.ReadFull(kdf, clientWriteKey); err != nil {
		t.Fatal(err)
	}
	serverReadAEAD := newGCM(t, clientWriteKey)

	srv := &fakeSyncdServer{t: t, cli: cli, name: name, patches: make(map[uint64][]byte)}
	httpSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{InsecureSkipVerify: true})
		if err != nil {
			t.Errorf("server: accept: %v", err)
			return
		}
		defer conn.CloseNow()
		var counter uint32
		for {
			_, msg, err := conn.Read(ctx)
			if err != nil {
				return
			}
			for len(msg) >= socket.FrameLengthSize {
				length := int(msg[0])<<16 | int(msg[1])<<8 | int(msg[2])
				frame := msg[socket.FrameLengthSize : socket.FrameLengthSize+length]
				msg = msg[socket.FrameLengthSize+length:]
				iv := make([]byte, 12)
				binary.BigEndian.PutUint32(iv[8:], counter)
				counter++
				plaintext, err := serverReadAEAD.Open(nil, iv, frame, nil)
				if err != nil {
					t.Errorf("server: decrypt frame: %v", err)
					return
				}
				unpacked, err := waBinary.Unpack(plaintext)
				if err != nil {
					t.Errorf("server: unpack frame: %v", err)
					return
				}
				node, err := waBinary.Unmarshal(unpacked)
				if err != nil {
					t.Errorf("server: unmarshal node: %v", err)
					return
				}
				if node.Tag == "iq" {
					// Each request on its own goroutine: a gated 409 must
					// not stall the fetch it is waiting for.
					go srv.handleIQ(ctx, node)
				}
			}
		}
	}))
	t.Cleanup(httpSrv.Close)

	fs := socket.NewFrameSocket(waLog.Noop, httpSrv.Client())
	fs.URL = "ws" + strings.TrimPrefix(httpSrv.URL, "http")
	fs.Header = nil
	if err := fs.Connect(ctx); err != nil {
		t.Fatalf("connect frame socket: %v", err)
	}
	nh := socket.NewNoiseHandshake()
	nh.Start(socket.NoiseStartPattern, socket.WAConnHeader)
	ns, err := nh.Finish(fs, func(context.Context, []byte) {}, func(context.Context, *socket.NoiseSocket, bool) {})
	if err != nil {
		t.Fatalf("finish noise socket: %v", err)
	}
	cli.socketLock.Lock()
	cli.socket = ns
	cli.socketLock.Unlock()
	t.Cleanup(func() {
		cli.socketLock.Lock()
		cli.socket = nil
		cli.socketLock.Unlock()
		ns.Stop(true, false)
	})
	return srv
}

// appState409RaceEnv is a client at version 100 of regular_low, with one
// archived chat, plus a server whose log already has version 101: another
// device re-archived the same chat (a SET that replaces an existing value,
// which is what makes a double application visible in the LTHash).
type appState409RaceEnv struct {
	cli    *Client
	server *fakeSyncdServer
	name   appstate.WAPatchName
	chat   types.JID
	other  types.JID
}

func newAppState409RaceEnv(t *testing.T, device *store.Device) *appState409RaceEnv {
	t.Helper()
	ctx := context.Background()
	const name = appstate.WAPatchRegularLow
	keyID := []byte{0, 0, 0, 0, 0xf3, 0xa6}
	keyData := make([]byte, 32)
	if _, err := rand.Read(keyData); err != nil {
		t.Fatal(err)
	}
	err := device.AppStateKeys.PutAppStateSyncKey(ctx, keyID, store.AppStateSyncKey{
		Data:        keyData,
		Fingerprint: []byte{1, 2, 3},
		Timestamp:   time.Now().UnixMilli(),
	})
	if err != nil {
		t.Fatalf("put app state key: %v", err)
	}
	cli := NewClient(device, waLog.Noop)

	env := &appState409RaceEnv{
		cli:   cli,
		name:  name,
		chat:  types.JID{User: "100000000000001", Server: types.HiddenUserServer},
		other: types.JID{User: "100000000000002", Server: types.HiddenUserServer},
	}
	env.server = attachFakeServer(t, cli, name)

	// Local state v100: the chat was archived once (value MAC vOld).
	seedPatch := appstate.BuildArchive(env.chat, true, time.Now().Add(-time.Hour), nil)
	var state appstate.HashState
	seedRaw, err := cli.appStateProc.EncodePatch(ctx, keyID, state, seedPatch)
	if err != nil {
		t.Fatalf("encode seed patch: %v", err)
	}
	var seed waServerSync.SyncdPatch
	if err = proto.Unmarshal(seedRaw, &seed); err != nil {
		t.Fatal(err)
	}
	seedMut := seed.GetMutations()[0]
	seedValue := seedMut.GetRecord().GetValue().GetBlob()
	seedMAC := store.AppStateMutationMAC{
		IndexMAC: seedMut.GetRecord().GetIndex().GetBlob(),
		ValueMAC: seedValue[len(seedValue)-32:],
	}
	state.Version = 100
	lthash.WAPatchIntegrity.SubtractThenAddInPlace(state.Hash[:], nil, [][]byte{seedMAC.ValueMAC})
	if err = device.AppState.PutAppStateVersion(ctx, string(name), state.Version, state.Hash); err != nil {
		t.Fatal(err)
	}
	if err = device.AppState.PutAppStateMutationMACs(ctx, string(name), state.Version, []store.AppStateMutationMAC{seedMAC}); err != nil {
		t.Fatal(err)
	}
	env.server.version = state.Version

	// Server v101, by WhatsApp Web (device 24): the same chat archived again
	// with a new timestamp, encoded on top of v100 exactly like that device
	// would (same keys, previous value MAC looked up in the same state).
	otherPatch := appstate.BuildArchive(env.chat, true, time.Now(), nil)
	otherRaw, err := cli.appStateProc.EncodePatch(ctx, keyID, state, otherPatch)
	if err != nil {
		t.Fatalf("encode other device's patch: %v", err)
	}
	var other waServerSync.SyncdPatch
	if err = proto.Unmarshal(otherRaw, &other); err != nil {
		t.Fatal(err)
	}
	other.DeviceIndex = proto.Uint32(24)
	other.ClientDebugData, err = proto.Marshal(&waSyncAction.PatchDebugData{
		SenderPlatform:  waSyncAction.PatchDebugData_WEB.Enum(),
		IsSenderPrimary: proto.Bool(false),
	})
	if err != nil {
		t.Fatal(err)
	}
	otherRaw, err = proto.Marshal(&other)
	if err != nil {
		t.Fatal(err)
	}
	env.server.appendPatch(otherRaw)
	if env.server.version != 101 {
		t.Fatalf("server version = %d, want 101", env.server.version)
	}
	return env
}

func (env *appState409RaceEnv) checkConsistent(t *testing.T, wantVersion uint64) {
	t.Helper()
	ctx := context.Background()
	version, hash, err := env.cli.Store.AppState.GetAppStateVersion(ctx, string(env.name))
	if err != nil {
		t.Fatalf("get version: %v", err)
	}
	if version != wantVersion {
		t.Errorf("stored version = %d, want %d", version, wantVersion)
	}
	if sum := sumStoredMACs(t, env.cli.Store.AppState, env.name); sum != hash {
		t.Errorf("stored LTHash %x… does not match the sum of the stored value MACs %x… (version %d)", hash[:8], sum[:8], version)
	}
}

// runAppState409RaceTests runs the two race scenarios against the given
// device constructor. The fetch and the conflicting send are both real
// client calls; only the server is fake.
func runAppState409RaceTests(t *testing.T, newDevice func(t *testing.T) *store.Device) {
	// Deterministic order: the notification-driven fetch applies v101 and
	// finishes before the 409 response reaches the client, so the 409 handler
	// runs with the version-100 state it read before sending. This is the
	// interleaving that corrupted the hash in production; before the fix
	// SendAppState fails and (when tolerated) the stored hash diverges.
	t.Run("fetch_applies_before_409_handler", func(t *testing.T) {
		env := newAppState409RaceEnv(t, newDevice(t))
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		fetchRan := make(chan struct{})
		var once sync.Once
		env.server.beforeConflict = func() {
			once.Do(func() {
				defer close(fetchRan)
				if err := env.cli.FetchAppState(ctx, env.name, false, false); err != nil {
					t.Errorf("FetchAppState: %v", err)
				}
			})
		}
		sendErr := env.cli.SendAppState(ctx, appstate.BuildArchive(env.other, true, time.Now(), nil))
		if sendErr != nil {
			t.Errorf("SendAppState after 409: %v", sendErr)
		}
		select {
		case <-fetchRan:
		case <-ctx.Done():
			t.Fatal("fetch never ran")
		}
		if got := env.server.conflicts.Load(); got != 1 {
			t.Errorf("server saw %d conflicts, want 1", got)
		}
		if got := env.server.sends.Load(); got != 2 {
			t.Errorf("server saw %d sends, want 2 (original + retry)", got)
		}
		// v101 (other device) + v102 (ours, accepted on retry).
		env.checkConsistent(t, 102)
	})

	// No ordering: the 409 response and the fetch race freely. Any
	// interleaving must leave version 102 with a hash equal to the stored
	// MACs and no error from either call.
	t.Run("concurrent", func(t *testing.T) {
		env := newAppState409RaceEnv(t, newDevice(t))
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		var wg sync.WaitGroup
		errs := make(chan error, 2)
		start := make(chan struct{})
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			if err := env.cli.SendAppState(ctx, appstate.BuildArchive(env.other, true, time.Now(), nil)); err != nil {
				errs <- fmt.Errorf("SendAppState: %w", err)
			}
		}()
		go func() {
			defer wg.Done()
			<-start
			if err := env.cli.FetchAppState(ctx, env.name, false, false); err != nil {
				errs <- fmt.Errorf("FetchAppState: %w", err)
			}
		}()
		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}
		env.checkConsistent(t, 102)
	})
}

func TestSendAppState409RaceWithNotificationSync(t *testing.T) {
	runAppState409RaceTests(t, newMemAppStateDevice)
}
