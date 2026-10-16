package whatsmeow

import (
	"context"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"go.mau.fi/whatsmeow/proto/waAdv"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
)

// newSQLiteAppStateDevice opens a fresh SQLite session store with the same
// connection parameters production uses, so the 409 race runs against the
// real schema (UNIQUE(jid, name, version, index_mac) on the MAC table) and
// the real transaction boundaries.
func newSQLiteAppStateDevice(t *testing.T) *store.Device {
	t.Helper()
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "session.db")
	container, err := sqlstore.New(ctx, "sqlite3", "file:"+dbPath+"?_foreign_keys=on", nil)
	if err != nil {
		t.Fatalf("open container: %v", err)
	}
	t.Cleanup(func() { container.Close() })
	device := container.NewDevice()
	device.ID = &types.JID{User: "15551234567", Device: 1, Server: types.DefaultUserServer}
	device.Account = &waAdv.ADVSignedDeviceIdentity{
		Details:             []byte{},
		AccountSignature:    make([]byte, 64),
		AccountSignatureKey: make([]byte, 32),
		DeviceSignature:     make([]byte, 64),
	}
	if err = container.PutDevice(ctx, device); err != nil {
		t.Fatalf("put device: %v", err)
	}
	return device
}

func TestSendAppState409RaceWithNotificationSyncSQLite(t *testing.T) {
	runAppState409RaceTests(t, newSQLiteAppStateDevice)
}
