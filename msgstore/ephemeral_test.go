package msgstore

import (
	"context"
	"testing"
	"time"

	"gopkg.in/irc.v4"

	"codeberg.org/emersion/soju/database"
)

func TestEphemeralStore_PurgeUser(t *testing.T) {
	if !database.SqliteEnabled {
		t.Skip("SQLite support is disabled")
	}

	db, err := database.OpenTempSqliteDB()
	if err != nil {
		t.Fatalf("failed to open temp sqlite db: %v", err)
	}
	defer db.Close()

	ctx := context.Background()

	u1 := database.NewUser("alice")
	if err := db.StoreUser(ctx, u1); err != nil {
		t.Fatalf("StoreUser u1: %v", err)
	}
	u2 := database.NewUser("bob")
	if err := db.StoreUser(ctx, u2); err != nil {
		t.Fatalf("StoreUser u2: %v", err)
	}

	n1 := database.NewNetwork("ircs://example.com")
	if err := db.StoreNetwork(ctx, u1.ID, n1); err != nil {
		t.Fatalf("StoreNetwork n1: %v", err)
	}
	n2 := database.NewNetwork("ircs://example.com")
	if err := db.StoreNetwork(ctx, u2.ID, n2); err != nil {
		t.Fatalf("StoreNetwork n2: %v", err)
	}

	// Pin a target for u1 to verify target metadata is preserved while messages are purged
	mt := &database.MessageTarget{Target: "#pinned", Pinned: true}
	if err := db.StoreMessageTarget(ctx, n1.ID, mt); err != nil {
		t.Fatalf("StoreMessageTarget: %v", err)
	}

	store := NewEphemeralStore(db)
	now := time.Now().UTC().Truncate(time.Millisecond)

	if _, err := store.Append(ctx, n1, "#chan", &irc.Message{
		Tags:    irc.Tags{"time": now.Format("2006-01-02T15:04:05.000Z")},
		Prefix:  &irc.Prefix{Name: "someone"},
		Command: "PRIVMSG",
		Params:  []string{"#chan", "hello alice"},
	}); err != nil {
		t.Fatalf("Append u1 #chan: %v", err)
	}
	if _, err := store.Append(ctx, n1, "#pinned", &irc.Message{
		Tags:    irc.Tags{"time": now.Format("2006-01-02T15:04:05.000Z")},
		Prefix:  &irc.Prefix{Name: "someone"},
		Command: "PRIVMSG",
		Params:  []string{"#pinned", "pinned hello"},
	}); err != nil {
		t.Fatalf("Append u1 #pinned: %v", err)
	}
	if _, err := store.Append(ctx, n2, "#chan", &irc.Message{
		Tags:    irc.Tags{"time": now.Format("2006-01-02T15:04:05.000Z")},
		Prefix:  &irc.Prefix{Name: "someone"},
		Command: "PRIVMSG",
		Params:  []string{"#chan", "hello bob"},
	}); err != nil {
		t.Fatalf("Append u2 #chan: %v", err)
	}

	// Store delivery receipt for u1
	if err := db.StoreClientDeliveryReceipts(ctx, n1.ID, "client1", []database.DeliveryReceipt{
		{Target: "#chan", InternalMsgID: "id1"},
	}); err != nil {
		t.Fatalf("StoreClientDeliveryReceipts: %v", err)
	}

	// Purge u1
	if err := store.PurgeUser(ctx, u1.ID); err != nil {
		t.Fatalf("PurgeUser: %v", err)
	}

	// u1 messages & FTS search should be empty
	msgs, err := store.LoadBeforeTime(ctx, now.Add(time.Minute), time.Time{}, &LoadMessageOptions{
		Network: n1,
		Entity:  "#chan",
		Limit:   100,
	})
	if err != nil {
		t.Fatalf("LoadBeforeTime u1: %v", err)
	}
	if len(msgs) != 0 {
		t.Fatalf("expected 0 messages for u1 #chan, got %d", len(msgs))
	}

	found, err := store.Search(ctx, n1, &SearchMessageOptions{
		In:    "#chan",
		Text:  "alice",
		Limit: 100,
	})
	if err != nil {
		t.Fatalf("Search u1: %v", err)
	}
	if len(found) != 0 {
		t.Fatalf("expected 0 search results for u1, got %d", len(found))
	}

	// Delivery receipts for n1 should be cleared
	rcpts, err := db.ListDeliveryReceipts(ctx, n1.ID)
	if err != nil {
		t.Fatalf("ListDeliveryReceipts: %v", err)
	}
	if len(rcpts) != 0 {
		t.Fatalf("expected 0 delivery receipts after purge, got %d", len(rcpts))
	}

	// Pinned target metadata for n1 should be preserved
	mts, err := db.ListMessageTargets(ctx, n1.ID)
	if err != nil {
		t.Fatalf("ListMessageTargets: %v", err)
	}
	if len(mts) != 1 || mts[0].Target != "#pinned" || !mts[0].Pinned {
		t.Fatalf("expected pinned target preserved, got %+v", mts)
	}

	// u2 messages must NOT be affected
	msgs2, err := store.LoadBeforeTime(ctx, now.Add(time.Minute), time.Time{}, &LoadMessageOptions{
		Network: n2,
		Entity:  "#chan",
		Limit:   100,
	})
	if err != nil {
		t.Fatalf("LoadBeforeTime u2: %v", err)
	}
	if len(msgs2) != 1 {
		t.Fatalf("expected 1 message for u2, got %d", len(msgs2))
	}
}
