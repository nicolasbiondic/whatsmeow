package sqlstore

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"go.mau.fi/whatsmeow/appstate/lthash"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
)

func seedResetBackup(t *testing.T, s *SQLStore) ([128]byte, []store.AppStateMutationMAC) {
	t.Helper()
	macs := []store.AppStateMutationMAC{{IndexMAC: mac(1), ValueMAC: mac(2)}}
	var hash [128]byte
	lthash.WAPatchIntegrity.SubtractThenAddInPlace(hash[:], nil, [][]byte{macs[0].ValueMAC})
	if err := s.ReplaceAppStateVersion(t.Context(), "regular", 100, hash, macs); err != nil {
		t.Fatal(err)
	}
	if err := s.PutAppStateBackup(t.Context(), "regular", 100, hash, macs); err != nil {
		t.Fatal(err)
	}
	return hash, macs
}

func assertResetRecovered(t *testing.T, s *SQLStore, hash [128]byte, macs []store.AppStateMutationMAC) {
	t.Helper()
	version, got, err := s.GetAppStateVersion(t.Context(), "regular")
	if err != nil || version != 100 || got != hash {
		t.Fatalf("state: version=%d hash_match=%t err=%v", version, got == hash, err)
	}
	gotMACs, err := s.GetAllAppStateMutationMACs(t.Context(), "regular")
	if err != nil || !reflect.DeepEqual(gotMACs, macs) {
		t.Fatalf("MACs: got=%v want=%v err=%v", gotMACs, macs, err)
	}
}

func TestAppStateRestoreTransactionFailureAtomic(t *testing.T) {
	for _, tc := range []struct{ name, trigger string }{
		{"delete", `BEFORE DELETE ON whatsmeow_app_state_version`},
		{"version", `BEFORE INSERT ON whatsmeow_app_state_version WHEN NEW.version=100`},
		{"hash", `BEFORE INSERT ON whatsmeow_app_state_version WHEN length(NEW.hash)=128`},
		{"macs", `BEFORE INSERT ON whatsmeow_app_state_mutation_macs`},
		{"retire_backup", `BEFORE DELETE ON whatsmeow_app_state_backup`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s := newTestAppStateStore(t)
			hash, macs := seedResetBackup(t, s)
			// A failed replacement must also leave a previously populated live
			// state intact, including all MACs deleted by its first statement.
			if _, err := s.db.Exec(t.Context(), `CREATE TRIGGER fail_restore `+tc.trigger+` BEGIN SELECT RAISE(ABORT, 'injected restore failure'); END`); err != nil {
				t.Fatal(err)
			}
			if err := s.ReplaceAppStateVersion(t.Context(), "regular", 100, hash, macs); err == nil {
				t.Fatal("restore unexpectedly succeeded")
			}
			assertResetRecovered(t, s, hash, macs)
			var count int
			if err := s.db.QueryRow(t.Context(), `SELECT COUNT(*) FROM whatsmeow_app_state_backup`).Scan(&count); err != nil || count != 1 {
				t.Fatalf("failed transaction lost durable backup: count=%d err=%v", count, err)
			}
			if _, err := s.db.Exec(t.Context(), `DROP TRIGGER fail_restore`); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestAppStateResetRestartRecoversVerifiedState(t *testing.T) {
	for _, phase := range []string{"before_reset", "after_reset", "version_without_macs"} {
		t.Run(phase, func(t *testing.T) {
			s := newTestAppStateStore(t)
			hash, macs := seedResetBackup(t, s)
			if phase != "before_reset" {
				if err := s.DeleteAppStateVersion(t.Context(), "regular"); err != nil {
					t.Fatal(err)
				}
			}
			if phase == "version_without_macs" {
				// Simulate an old writer interrupted in its multistatement restore.
				if err := s.PutAppStateVersion(t.Context(), "regular", 100, hash); err != nil {
					t.Fatal(err)
				}
			}
			var sequence int
			var databaseName, path string
			if err := s.db.QueryRow(t.Context(), `PRAGMA database_list`).Scan(&sequence, &databaseName, &path); err != nil {
				t.Fatal(err)
			}
			// Discard every in-memory object and connection without invoking
			// restore. Only committed SQL survives this simulated process death.
			if err := s.Container.Close(); err != nil {
				t.Fatal(err)
			}
			c, err := New(t.Context(), "sqlite3", "file:"+path+"?_foreign_keys=on", nil)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			jid, err := types.ParseJID(s.JID)
			if err != nil {
				t.Fatal(err)
			}
			assertResetRecovered(t, NewSQLStore(c, jid), hash, macs)
		})
	}
}

func TestAppStateRestartRecoveryFailsClosedAndRetries(t *testing.T) {
	s := newTestAppStateStore(t)
	hash, macs := seedResetBackup(t, s)
	if err := s.DeleteAppStateVersion(t.Context(), "regular"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec(t.Context(), `CREATE TRIGGER fail_reopen BEFORE INSERT ON whatsmeow_app_state_mutation_macs BEGIN SELECT RAISE(ABORT, 'injected reopen failure'); END`); err != nil {
		t.Fatal(err)
	}
	jid, _ := types.ParseJID(s.JID)
	reopened := NewSQLStore(s.Container, jid)
	if v, _, err := reopened.GetAppStateVersion(context.Background(), "regular"); err == nil || v != 0 {
		t.Fatal(fmt.Sprintf("incomplete restore exposed state: version=%d err=%v", v, err))
	}
	if _, err := s.db.Exec(t.Context(), `DROP TRIGGER fail_reopen`); err != nil {
		t.Fatal(err)
	}
	assertResetRecovered(t, reopened, hash, macs)
}
