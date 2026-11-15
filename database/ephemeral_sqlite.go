//go:build !nosqlite

package database

import "context"

func (db *SqliteDB) DeleteUserMessages(ctx context.Context, userID int64) error {
	ctx, cancel := context.WithTimeout(ctx, sqliteQueryTimeout)
	defer cancel()

	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, `
		DELETE FROM DeliveryReceipt
		WHERE network IN (SELECT id FROM Network WHERE user = ?)`, userID)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, `
		DELETE FROM Message
		WHERE target IN (
			SELECT MessageTarget.id
			FROM MessageTarget
			JOIN Network ON MessageTarget.network = Network.id
			WHERE Network.user = ?
		)`, userID)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, `
		DELETE FROM MessageTarget
		WHERE network IN (SELECT id FROM Network WHERE user = ?)
		AND NOT pinned AND NOT muted AND NOT blocked`, userID)
	if err != nil {
		return err
	}

	return tx.Commit()
}
