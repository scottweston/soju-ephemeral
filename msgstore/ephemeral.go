package msgstore

import (
	"context"
	"fmt"

	"codeberg.org/emersion/soju/database"
)

const DriverEphemeral = Driver("ephemeral")

// EphemeralStore is a message store that supports purging all messages for a user.
type EphemeralStore interface {
	Store
	PurgeUser(ctx context.Context, userID int64) error
}

type ephemeralMessageStore struct {
	*dbMessageStore
}

var (
	_ Store            = (*ephemeralMessageStore)(nil)
	_ ChatHistoryStore = (*ephemeralMessageStore)(nil)
	_ SearchStore      = (*ephemeralMessageStore)(nil)
	_ EphemeralStore   = (*ephemeralMessageStore)(nil)
)

func NewEphemeralStore(db database.Database) *ephemeralMessageStore {
	return &ephemeralMessageStore{
		dbMessageStore: NewDBStore(db),
	}
}

func (ms *ephemeralMessageStore) PurgeUser(ctx context.Context, userID int64) error {
	edb, ok := ms.db.(database.EphemeralDatabase)
	if !ok {
		return fmt.Errorf("database backend does not support ephemeral message purging")
	}
	return edb.DeleteUserMessages(ctx, userID)
}
