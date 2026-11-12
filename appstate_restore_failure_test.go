package whatsmeow

import (
	"context"
	"errors"
	"testing"
	"time"

	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
)

type failingAtomicRestoreStore struct {
	*cancelAfterResetStore
	failure error
	cap     bool
}

func (s *failingAtomicRestoreStore) ReplaceAppStateVersion(ctx context.Context, name string, version uint64, hash [128]byte, macs []store.AppStateMutationMAC) error {
	if s.cap {
		<-ctx.Done()
		return s.AppStateStore.ReplaceAppStateVersion(ctx, name, version, hash, macs)
	}
	return s.failure
}

func TestFullSyncRestoreFailureIsJoinedAndRecoverable(t *testing.T) {
	for _, cap := range []bool{false, true} {
		name := "error"
		if cap {
			name = "real_ten_second_cap"
		}
		t.Run(name, func(t *testing.T) {
			device := newSQLiteAppStateDevice(t)
			env := newAppState409RaceEnv(t, device)
			original := device.AppState.(*sqlstore.SQLStore)
			before := sumStoredMACs(t, original, env.name)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			injected := errors.New("injected atomic restore failure")
			device.AppState = &failingAtomicRestoreStore{
				cancelAfterResetStore: &cancelAfterResetStore{AppStateStore: original, cancel: cancel},
				failure:               injected, cap: cap,
			}
			started := time.Now()
			err := env.cli.FetchAppState(ctx, env.name, true, false)
			wantRestoreError := injected
			if cap {
				wantRestoreError = context.DeadlineExceeded
				if elapsed := time.Since(started); elapsed < 10*time.Second {
					t.Fatalf("did not exercise real cleanup cap: %v", elapsed)
				}
			}
			if !errors.Is(err, context.Canceled) || !errors.Is(err, wantRestoreError) {
				t.Fatalf("fetch must expose original and restore errors: %v", err)
			}
			version, _, readErr := original.GetAppStateVersion(t.Context(), string(env.name))
			if readErr != nil || version != 0 {
				t.Fatalf("failed restore exposed a valid-looking version: %d %v", version, readErr)
			}
			jid, _ := types.ParseJID(original.JID)
			reopened := sqlstore.NewSQLStore(original.Container, jid)
			version, hash, readErr := reopened.GetAppStateVersion(t.Context(), string(env.name))
			if readErr != nil || version != 100 || hash != before || hash != sumStoredMACs(t, reopened, env.name) {
				t.Fatalf("restart lost verified state: version=%d err=%v", version, readErr)
			}
		})
	}
}
