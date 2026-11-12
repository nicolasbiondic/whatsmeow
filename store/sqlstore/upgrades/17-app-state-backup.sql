-- v17 (compatible with v8+): Preserve verified app state across interrupted resets
CREATE TABLE whatsmeow_app_state_backup (
	jid TEXT NOT NULL,
	name TEXT NOT NULL,
	version BIGINT NOT NULL,
	hash bytea NOT NULL CHECK ( length(hash) = 128 ),
	macs bytea NOT NULL,
	PRIMARY KEY (jid, name),
	FOREIGN KEY (jid) REFERENCES whatsmeow_device(jid) ON DELETE CASCADE ON UPDATE CASCADE
);
