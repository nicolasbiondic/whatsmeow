package whatsmeow

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"go.mau.fi/whatsmeow/appstate"
	"go.mau.fi/whatsmeow/appstate/lthash"
	waBinary "go.mau.fi/whatsmeow/binary"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/proto/waSyncAction"
	"go.mau.fi/whatsmeow/proto/waSyncdSnapshotRecovery"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
)

// errorLog keeps what the client logs at ERROR, the only level applications
// commonly keep for it.
type errorLog struct {
	waLog.Logger
	mu    sync.Mutex
	lines []string
}

func (l *errorLog) Errorf(msg string, args ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.lines = append(l.lines, fmt.Sprintf(msg, args...))
}
func (l *errorLog) Warnf(string, ...any)    {}
func (l *errorLog) Infof(string, ...any)    {}
func (l *errorLog) Debugf(string, ...any)   {}
func (l *errorLog) Sub(string) waLog.Logger { return l }
func (l *errorLog) get() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.lines...)
}

// backlogNotices records what the client tells the application about its
// event backlog.
type backlogNotices struct {
	mu     sync.Mutex
	events []any
}

func watchBacklog(cli *Client) *backlogNotices {
	n := &backlogNotices{}
	cli.AddEventHandler(func(evt any) {
		switch evt.(type) {
		case *events.AppStateDeferred, *events.AppStateCaughtUp, *events.AppStateCatchUpFailed:
			n.mu.Lock()
			n.events = append(n.events, evt)
			n.mu.Unlock()
		}
	})
	return n
}

func noticesOf[T any](n *backlogNotices) (out []T) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for _, evt := range n.events {
		if typed, ok := evt.(T); ok {
			out = append(out, typed)
		}
	}
	return out
}

func (n *backlogNotices) deferred() []*events.AppStateDeferred {
	return noticesOf[*events.AppStateDeferred](n)
}
func (n *backlogNotices) caughtUp() []*events.AppStateCaughtUp {
	return noticesOf[*events.AppStateCaughtUp](n)
}
func (n *backlogNotices) failed() []*events.AppStateCatchUpFailed {
	return noticesOf[*events.AppStateCatchUpFailed](n)
}

func eventually(t *testing.T, ctx context.Context, what string, done func() bool) {
	t.Helper()
	for !done() {
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s", what)
		case <-time.After(2 * time.Millisecond):
		}
	}
}

func labelNotification(version uint64) *waBinary.Node {
	return &waBinary.Node{Tag: "notification", Content: []waBinary.Node{{
		Tag: "collection", Attrs: waBinary.Attrs{"name": "regular", "version": fmt.Sprint(version)},
	}}}
}

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
// before anything is sent or stored. Nothing waits, nothing that was
// committed is dropped, every refusal is logged at ERROR and reported while
// the handler is still held, and the client fetches what was refused by
// itself once the handlers have caught up.
func TestAppStateBacklogBoundsAdmission(t *testing.T) {
	cli, server := reviewLabelClient(t)
	cli.AppStateBacklogLimit = 3
	log := &errorLog{}
	cli.Log = log
	notices := watchBacklog(cli)
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
	refused := []struct {
		source string
		err    error
	}{
		{"fetch", cli.FetchAppState(ctx, appstate.WAPatchRegular, false, false)},
		{"fetch", func() error {
			_, err := cli.DangerousInternals().FetchAppState(ctx, appstate.WAPatchRegular, false, false)
			return err
		}()},
		{"ordered fetch", cli.DangerousInternals().FetchAppStateOrdered(ctx, appstate.WAPatchRegular, false, false, func([]any) { t.Error("refused ordered fetch delivered") })},
		{"send", cli.SendAppState(ctx, appstate.BuildLabelEdit("7", "local", 1, false))},
	}
	for _, r := range refused {
		var backlog *AppStateBacklogError
		if !errors.Is(r.err, ErrAppStateBacklog) || !errors.As(r.err, &backlog) || backlog.Name != appstate.WAPatchRegular || backlog.Depth != 3 {
			t.Fatalf("%s at the limit: %v", r.source, r.err)
		}
	}
	if !cli.handleAppStateRecovery(ctx, "deferred-recovery", labelRecovery(t, cli, 120, "recovered")) {
		t.Fatal("a recovery deferred by the backlog must be handled: the client keeps the snapshot")
	}
	if !cli.handleAppStateNotification(ctx, labelNotification(112)) {
		t.Fatal("a refused notification must be consumed: its fetch is owed")
	}
	version, _, err := cli.Store.AppState.GetAppStateVersion(ctx, string(appstate.WAPatchRegular))
	if err != nil || version != 111 || server.sends.Load() != 0 {
		t.Fatalf("refused work was stored or sent: version=%d err=%v sends=%d", version, err, server.sends.Load())
	}
	if backlog := cli.AppStateBacklog(appstate.WAPatchRegular); backlog.Depth != 3 {
		t.Fatalf("refused work kept capacity: %+v", backlog)
	}
	other := &appStateBatch{name: appstate.WAPatchRegularLow}
	if err := cli.claimAppStateBatch(other); err != nil {
		t.Fatalf("another collection was held back: %v", err)
	}
	cli.finishAppStateBatch(other, nil)

	// Every refusal is logged at ERROR, and the application is told while the
	// handler is still held. Notices that are not delivered yet are coalesced,
	// so only the latest one is certain to arrive.
	wantSources := []string{"fetch", "fetch", "ordered fetch", "send", "recovery", "notification"}
	lines := log.get()
	if len(lines) != len(wantSources) {
		t.Fatalf("ERROR lines: %q", lines)
	}
	for i, source := range wantSources {
		if !strings.Contains(lines[i], "App state regular was not synced ("+source+"): 3 event batches") {
			t.Fatalf("ERROR line %d: %q, want source %q", i, lines[i], source)
		}
	}
	if !strings.Contains(lines[4], "notified version 120") || !strings.Contains(lines[5], "notified version 112") {
		t.Fatalf("ERROR lines lack the versions: %q", lines[4:])
	}
	eventually(t, ctx, "the latest refusal to be reported", func() bool {
		deferred := notices.deferred()
		return len(deferred) > 0 && deferred[len(deferred)-1].Source == "notification"
	})
	if last := notices.deferred()[len(notices.deferred())-1]; last.Name != appstate.WAPatchRegular || last.Version != 112 || last.Depth != 3 || last.OldestAge <= 0 {
		t.Fatalf("refusal reported as %+v", last)
	}
	if len(seen.get()) != 0 || len(notices.caughtUp()) != 0 {
		t.Fatalf("delivered %v past the held batch", seen.get())
	}

	fetches := server.fetches.Load()
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
	// The client made up for it by itself: it adopted the snapshot it kept,
	// which supersedes "later", and then did the one fetch it owed.
	if got := seen.get(); !reflect.DeepEqual(got, []string{"old", "new", "newest", "recovered"}) {
		t.Fatalf("labels delivered as %v", got)
	}
	if labelVersion(t, cli) != 120 || len(notices.caughtUp()) == 0 || server.fetches.Load() != fetches+1 || len(notices.failed()) != 0 {
		t.Fatalf("catch-up: version=%d caughtUp=%+v, %d fetches", labelVersion(t, cli), notices.caughtUp(), server.fetches.Load()-fetches)
	}
	if err := cli.FetchAppState(ctx, appstate.WAPatchRegular, false, false); err != nil {
		t.Fatalf("fetch after the drain: %v", err)
	}
}

// The handler whose own batch fills the backlog gets the error at once, and
// what it asked for is fetched after it returned.
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
			if version, _, err := cli.Store.AppState.GetAppStateVersion(ctx, string(appstate.WAPatchRegular)); err != nil || version != 101 {
				t.Errorf("refused nested fetch stored something: version=%d err=%v", version, err)
			}
		}
	})
	if err := cli.FetchAppState(ctx, appstate.WAPatchRegular, false, false); err != nil {
		t.Fatal(err)
	}
	if err := <-nested; !errors.Is(err, ErrAppStateBacklog) {
		t.Fatalf("nested fetch at the limit: %v", err)
	}
	if err := cli.WaitForAppStateEvents(ctx); err != nil {
		t.Fatal(err)
	}
	if version, _, err := cli.Store.AppState.GetAppStateVersion(ctx, string(appstate.WAPatchRegular)); err != nil || version != 102 {
		t.Fatalf("version=%d err=%v, want the refused patch fetched after the handler returned", version, err)
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
