package whatsmeow

import (
	"context"

	"go.mau.fi/whatsmeow/appstate"
)

// IsAppStateCollectionLIDMigrated reports whether the local state of an app
// state collection holds chat entries indexed by LID, i.e. the primary
// migrated the account's chats to LIDs. Callers use it to write and sync the
// way WhatsApp Web does on such accounts, without full resyncs before writes.
func (cli *Client) IsAppStateCollectionLIDMigrated(ctx context.Context, name appstate.WAPatchName) bool {
	if cli == nil || cli.appStateProc == nil {
		return false
	}
	return cli.appStateProc.IsCollectionLIDMigrated(ctx, name)
}
