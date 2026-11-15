package database

import "context"

// EphemeralDatabase is implemented by database backends that support purging
// all stored messages for a single user.
type EphemeralDatabase interface {
	Database
	DeleteUserMessages(ctx context.Context, userID int64) error
}

func (db *PostgresDB) DeleteUserMessages(ctx context.Context, userID int64) error {
	ctx, cancel := context.WithTimeout(ctx, postgresQueryTimeout)
	defer cancel()

	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, `
		DELETE FROM "DeliveryReceipt"
		WHERE network IN (SELECT id FROM "Network" WHERE "user" = $1)`, userID)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, `
		DELETE FROM "Message"
		WHERE target IN (
			SELECT "MessageTarget".id
			FROM "MessageTarget"
			JOIN "Network" ON "MessageTarget".network = "Network".id
			WHERE "Network"."user" = $1
		)`, userID)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, `
		DELETE FROM "MessageTarget"
		WHERE network IN (SELECT id FROM "Network" WHERE "user" = $1)
		AND NOT pinned AND NOT muted AND NOT blocked`, userID)
	if err != nil {
		return err
	}

	return tx.Commit()
}
