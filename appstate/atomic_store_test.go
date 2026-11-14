package appstate

import (
	"context"

	"go.mau.fi/whatsmeow/store"
)

func (f *memAppStateStore) PutAppStateBackup(context.Context, string, uint64, [128]byte, []store.AppStateMutationMAC) error {
	return nil
}

func (f *memAppStateStore) ReplaceAppStateVersion(ctx context.Context, name string, version uint64, hash [128]byte, macs []store.AppStateMutationMAC) error {
	next := newMemAppStateStore()
	if err := next.PutAppStateMutationMACs(ctx, name, version, macs); err != nil {
		return err
	}
	f.versions[name], f.hashes[name], f.macs[name] = version, hash, next.macs[name]
	return nil
}

func (f *memAppStateStore) RecoverAppStateBackup(context.Context, string) error { return nil }

func (f *memAppStateStore) DeleteAppStateBackup(context.Context, string) error { return nil }
