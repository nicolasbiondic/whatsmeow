package sqlstore

import (
	"database/sql"
	"os"
	"os/exec"
	"testing"

	"go.mau.fi/whatsmeow/types"
)

func TestReviewR2AbruptProcessExit(t *testing.T) {
	if path := os.Getenv("REVIEW_R2_SYNTHETIC_DB"); path != "" {
		db, err := sql.Open("sqlite3", "file:"+path+"?_foreign_keys=on")
		if err != nil {
			t.Fatal(err)
		}
		if os.Getenv("REVIEW_R2_CRASH_PHASE") == "inside_replace" {
			tx, err := db.Begin()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec("DELETE FROM whatsmeow_app_state_version"); err != nil {
				t.Fatal(err)
			}
			os.Exit(73)
		}
		if _, err := db.Exec("DELETE FROM whatsmeow_app_state_version"); err != nil {
			t.Fatal(err)
		}
		os.Exit(73)
	}
	for _, phase := range []string{"after_reset", "inside_replace"} {
		t.Run(phase, func(t *testing.T) {
			s := newTestAppStateStore(t)
			hash, macs := seedResetBackup(t, s)
			var sequence int
			var dbName, path string
			if err := s.db.QueryRow(t.Context(), "PRAGMA database_list").Scan(&sequence, &dbName, &path); err != nil {
				t.Fatal(err)
			}
			if err := s.Container.Close(); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestReviewR2AbruptProcessExit$")
			cmd.Env = append(os.Environ(), "REVIEW_R2_SYNTHETIC_DB="+path, "REVIEW_R2_CRASH_PHASE="+phase)
			err := cmd.Run()
			exit, ok := err.(*exec.ExitError)
			if !ok || exit.ExitCode() != 73 {
				t.Fatal("child did not reach the intended abrupt-exit boundary")
			}
			c, err := New(t.Context(), "sqlite3", "file:"+path+"?_foreign_keys=on", nil)
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			jid, _ := types.ParseJID(s.JID)
			reopened := NewSQLStore(c, jid)
			assertResetRecovered(t, reopened, hash, macs)
			t.Log("abrupt exit recovered complete original state")
		})
	}
}
