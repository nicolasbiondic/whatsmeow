package sqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"go.mau.fi/whatsmeow/appstate/lthash"
	"go.mau.fi/whatsmeow/store"
)

const deleteAppStateBackupQuery = `DELETE FROM whatsmeow_app_state_backup WHERE jid=$1 AND name=$2`

type appStateRecoveryCheck struct {
	sync.Mutex
	done bool
}

// PutAppStateBackup commits before the destructive reset. The backup does not
// reference the version row, so deleting that row cannot cascade into it.
func (s *SQLStore) PutAppStateBackup(ctx context.Context, name string, version uint64, hash [128]byte, macs []store.AppStateMutationMAC) error {
	if err := s.recoverAppStateBackup(ctx, name); err != nil {
		return err
	}
	raw, err := json.Marshal(macs)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(ctx, `INSERT INTO whatsmeow_app_state_backup (jid,name,version,hash,macs)
		VALUES ($1,$2,$3,$4,$5) ON CONFLICT (jid,name) DO NOTHING`, s.JID, name, version, hash[:], raw)
	return err
}

func (s *SQLStore) ReplaceAppStateVersion(ctx context.Context, name string, version uint64, hash [128]byte, macs []store.AppStateMutationMAC) error {
	return s.db.DoTxn(ctx, nil, func(ctx context.Context) error {
		if err := s.DeleteAppStateVersion(ctx, name); err != nil {
			return err
		}
		if err := s.PutAppStateVersion(ctx, name, version, hash); err != nil {
			return err
		}
		if err := s.PutAppStateMutationMACs(ctx, name, version, macs); err != nil {
			return err
		}
		_, err := s.db.Exec(ctx, deleteAppStateBackupQuery, s.JID, name)
		return err
	})
}

// Recover once per collection/store instance, before the first version read.
// A failed transaction leaves the journal intact and the read fails closed;
// subsequent reads may retry. No partially restored version is exposed.
func (s *SQLStore) recoverAppStateBackup(ctx context.Context, name string) error {
	value, _ := s.appStateRecoveryChecks.LoadOrStore(name, &appStateRecoveryCheck{})
	check := value.(*appStateRecoveryCheck)
	check.Lock()
	defer check.Unlock()
	if check.done {
		return nil
	}
	err := s.db.DoTxn(ctx, nil, func(ctx context.Context) error {
		var version uint64
		var hashBytes, raw []byte
		err := s.db.QueryRow(ctx, `SELECT version,hash,macs FROM whatsmeow_app_state_backup WHERE jid=$1 AND name=$2`, s.JID, name).Scan(&version, &hashBytes, &raw)
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		} else if err != nil {
			return err
		}
		if len(hashBytes) != 128 || version == 0 {
			return fmt.Errorf("invalid app state reset backup for %s", name)
		}
		var macs []store.AppStateMutationMAC
		if err := json.Unmarshal(raw, &macs); err != nil {
			return fmt.Errorf("decode app state reset backup: %w", err)
		}
		hash := *(*[128]byte)(hashBytes)
		if !appStateMACsMatch(hash, macs) {
			return fmt.Errorf("app state reset backup integrity failure for %s", name)
		}
		currentVersion, currentHash, err := s.getAppStateVersion(ctx, name)
		if err != nil {
			return err
		}
		if currentVersion >= version {
			currentMACs, err := s.GetAllAppStateMutationMACs(ctx, name)
			if err != nil {
				return err
			}
			if appStateMACsMatch(currentHash, currentMACs) {
				_, err = s.db.Exec(ctx, deleteAppStateBackupQuery, s.JID, name)
				return err
			}
		}
		return s.ReplaceAppStateVersion(ctx, name, version, hash, macs)
	})
	if err == nil {
		check.done = true
	}
	return err
}

func appStateMACsMatch(hash [128]byte, macs []store.AppStateMutationMAC) bool {
	values := make([][]byte, len(macs))
	for i, mac := range macs {
		if len(mac.IndexMAC) != 32 || len(mac.ValueMAC) != 32 {
			return false
		}
		values[i] = mac.ValueMAC
	}
	var computed [128]byte
	lthash.WAPatchIntegrity.SubtractThenAddInPlace(computed[:], nil, values)
	return computed == hash
}
