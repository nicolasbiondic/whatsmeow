package whatsmeow

import (
	"bytes"
	"context"

	"go.mau.fi/whatsmeow/store"
)

func (m *memAppStateStore) PutAppStateBackup(context.Context, string, uint64, [128]byte, []store.AppStateMutationMAC) error {
	// In-memory test stores have no restart durability; the fetch retains its
	// backup until completion. SQL durability is covered separately.
	return nil
}

func (m *memAppStateStore) ReplaceAppStateVersion(_ context.Context, name string, version uint64, hash [128]byte, macs []store.AppStateMutationMAC) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.versions[name], m.hashes[name] = version, hash
	m.macs[name] = make(map[[32]byte]map[uint64][]byte, len(macs))
	for _, mac := range macs {
		m.macs[name][[32]byte(mac.IndexMAC)] = map[uint64][]byte{version: bytes.Clone(mac.ValueMAC)}
	}
	return nil
}

func (f *fakeAppStateVersionStore) PutAppStateBackup(context.Context, string, uint64, [128]byte, []store.AppStateMutationMAC) error {
	return nil
}

func (f *fakeAppStateVersionStore) ReplaceAppStateVersion(_ context.Context, _ string, version uint64, hash [128]byte, macs []store.AppStateMutationMAC) error {
	f.version, f.hash = version, hash
	f.macs = make(map[string][]byte, len(macs))
	for _, mac := range macs {
		f.macs[string(mac.IndexMAC)] = bytes.Clone(mac.ValueMAC)
	}
	return nil
}

// These stores keep no reset journal: there is nothing to recover or retire.
func (m *memAppStateStore) RecoverAppStateBackup(context.Context, string) error { return nil }

func (m *memAppStateStore) DeleteAppStateBackup(context.Context, string) error { return nil }

func (f *fakeAppStateVersionStore) RecoverAppStateBackup(context.Context, string) error { return nil }

func (f *fakeAppStateVersionStore) DeleteAppStateBackup(context.Context, string) error { return nil }
