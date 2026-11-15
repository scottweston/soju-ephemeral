package soju

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"gopkg.in/irc.v4"

	"codeberg.org/emersion/soju/config"
	"codeberg.org/emersion/soju/database"
	"codeberg.org/emersion/soju/msgstore"
	"codeberg.org/emersion/soju/xirc"
)

func newEphemeralTestServer(t *testing.T, db database.Database, grace, history time.Duration, quitMsg string) *Server {
	t.Helper()
	srv := NewServer(db)
	srv.Logger = newDebugLogger(t)
	cfg := *srv.Config()
	cfg.MsgStore = config.MsgStore{Driver: msgstore.DriverEphemeral}
	cfg.EphemeralMode = true
	cfg.EphemeralGrace = grace
	cfg.EphemeralHistory = history
	cfg.EphemeralQuitMsg = quitMsg
	srv.SetConfig(&cfg)
	return srv
}

func acceptWithin(t *testing.T, ln net.Listener, timeout time.Duration) (ircConn, bool) {
	t.Helper()
	if tl, ok := ln.(*net.TCPListener); ok {
		if err := tl.SetDeadline(time.Now().Add(timeout)); err != nil {
			t.Fatalf("SetDeadline failed: %v", err)
		}
		defer tl.SetDeadline(time.Time{})
	}
	c, err := ln.Accept()
	if err != nil {
		return nil, false
	}
	return newNetIRCConn(c), true
}

func readUntilCommand(t *testing.T, c ircConn, cmd string, timeout time.Duration) (*irc.Message, bool) {
	t.Helper()
	ch := make(chan *irc.Message, 16)
	go func() {
		for {
			msg, err := c.ReadMessage()
			if err != nil {
				close(ch)
				return
			}
			ch <- msg
			if msg.Command == cmd {
				return
			}
		}
	}()
	deadline := time.After(timeout)
	for {
		select {
		case msg, ok := <-ch:
			if !ok {
				return nil, false
			}
			if msg.Command == cmd {
				return msg, true
			}
		case <-deadline:
			return nil, false
		}
	}
}

func expectClosed(t *testing.T, c ircConn, timeout time.Duration) {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		for {
			_, err := c.ReadMessage()
			if err != nil {
				done <- err
				return
			}
		}
	}()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected connection error/EOF, got nil")
		}
	case <-time.After(timeout):
		t.Fatal("timed out waiting for connection to close")
	}
}

func TestEphemeral_NoUpstreamConnectOnStartupAndNoPurgeOnRestart(t *testing.T) {
	db := createTempSqliteDB(t)
	user := createTestUser(t, db)
	network, upstream := createTestUpstream(t, db, user)
	defer upstream.Close()

	// Seed an existing message before startup (simulating crash/restart)
	store := msgstore.NewEphemeralStore(db)
	_, err := store.Append(context.Background(), network, "foo", &irc.Message{
		Tags:    irc.Tags{"time": xirc.FormatServerTime(time.Now())},
		Prefix:  &irc.Prefix{Name: "foo"},
		Command: "PRIVMSG",
		Params:  []string{testUsername, "before restart"},
	})
	if err != nil {
		t.Fatalf("Append failed: %v", err)
	}

	srv := newEphemeralTestServer(t, db, 50*time.Millisecond, 24*time.Hour, "BYE")
	if err := srv.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	var uc ircConn
	t.Cleanup(func() {
		if uc != nil {
			uc.Close()
		}
	})
	defer srv.Shutdown()

	// Upstream MUST NOT receive a connection on startup
	if c, ok := acceptWithin(t, upstream, 150*time.Millisecond); ok {
		c.Close()
		t.Fatal("upstream connected before any client authenticated")
	}

	// Connect and authenticate a downstream client -> upstream should now connect
	dc := createTestDownstream(t, srv)
	defer dc.Close()
	registerDownstreamConn(t, dc, network)

	var ok bool
	uc, ok = acceptWithin(t, upstream, 2*time.Second)
	if !ok {
		t.Fatal("expected upstream connection after downstream authenticated")
	}
	registerUpstreamConn(t, uc)

	// Existing message from before restart should NOT have been purged on startup
	roundtrip(t, dc)
	dc.WriteMessage(&irc.Message{
		Command: "CHATHISTORY",
		Params:  []string{"LATEST", "foo", "*", "10"},
	})
	var got []string
	for _, m := range roundtrip(t, dc) {
		if m.Command == "PRIVMSG" {
			got = append(got, m.Params[1])
		}
	}
	if len(got) != 1 || got[0] != "before restart" {
		t.Fatalf("expected pre-restart message preserved, got %v", got)
	}
}

func TestEphemeral_GraceTimerExpiresAndPurgesAndReconnects(t *testing.T) {
	db := createTempSqliteDB(t)
	user := createTestUser(t, db)
	network, upstream := createTestUpstream(t, db, user)
	defer upstream.Close()

	// Also add a disabled network to verify it stays disabled on re-auth
	disabledNet := database.NewNetwork("irc+insecure://127.0.0.1:1")
	disabledNet.Name = "disablednet"
	disabledNet.Enabled = false
	if err := db.StoreNetwork(context.Background(), user.ID, disabledNet); err != nil {
		t.Fatalf("StoreNetwork disabled: %v", err)
	}

	srv := newEphemeralTestServer(t, db, 120*time.Millisecond, 24*time.Hour, "SIGNAL·DROPPED")
	if err := srv.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer srv.Shutdown()

	dc := createTestDownstream(t, srv)
	registerDownstreamConn(t, dc, network)

	uc, ok := acceptWithin(t, upstream, 2*time.Second)
	if !ok {
		t.Fatal("expected upstream connection")
	}
	registerUpstreamConn(t, uc)
	roundtrip(t, uc)
	roundtrip(t, dc)

	// Receive a message from upstream
	uc.WriteMessage(&irc.Message{
		Tags:    irc.Tags{"time": xirc.FormatServerTime(time.Now())},
		Prefix:  &irc.Prefix{Name: "alice"},
		Command: "PRIVMSG",
		Params:  []string{testUsername, "ephemeral message"},
	})
	if _, ok := readUntilCommand(t, dc, "PRIVMSG", 2*time.Second); !ok {
		t.Fatal("did not receive PRIVMSG downstream")
	}

	// Bare /quit disconnects only downstream; grace timer starts
	dc.WriteMessage(&irc.Message{Command: "QUIT"})
	expectClosed(t, dc, 2*time.Second)

	// Upstream should receive QUIT with configured quit message after grace timer expires
	quitMsg, ok := readUntilCommand(t, uc, "QUIT", 2*time.Second)
	if !ok {
		t.Fatal("expected upstream QUIT after grace timer expired")
	}
	if len(quitMsg.Params) != 1 || quitMsg.Params[0] != "SIGNAL·DROPPED" {
		t.Fatalf("got QUIT params=%v, want [SIGNAL·DROPPED]", quitMsg.Params)
	}
	expectClosed(t, uc, 2*time.Second)

	// Reconnect downstream (hibernated/disabled user should be re-enabled on auth!)
	dc2 := createTestDownstream(t, srv)
	defer dc2.Close()
	registerDownstreamConn(t, dc2, network)

	uc2, ok := acceptWithin(t, upstream, 2*time.Second)
	if !ok {
		t.Fatal("expected upstream to reconnect on client re-auth")
	}
	defer uc2.Close()
	registerUpstreamConn(t, uc2)
	roundtrip(t, dc2)

	// Verify CHATHISTORY is empty (purged!)
	dc2.WriteMessage(&irc.Message{
		Command: "CHATHISTORY",
		Params:  []string{"LATEST", "alice", "*", "10"},
	})
	for _, m := range roundtrip(t, dc2) {
		if m.Command == "PRIVMSG" {
			t.Fatalf("expected purged chathistory, got %v", m)
		}
	}

	// Verify disabled network remained disabled
	u := srv.getUser(testUsername)
	if u == nil {
		t.Fatal("expected user loaded")
	}
	dn := u.getNetwork("disablednet")
	if dn == nil || dn.Enabled || dn.conn != nil {
		t.Fatalf("expected disablednet to remain disabled and disconnected, got %+v", dn)
	}
}

func TestEphemeral_GraceTimerCancelledOnReconnect(t *testing.T) {
	db := createTempSqliteDB(t)
	user := createTestUser(t, db)
	network, upstream := createTestUpstream(t, db, user)
	defer upstream.Close()

	srv := newEphemeralTestServer(t, db, 1200*time.Millisecond, 24*time.Hour, "BYE")
	if err := srv.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	var uc ircConn
	t.Cleanup(func() {
		if uc != nil {
			uc.Close()
		}
	})
	defer srv.Shutdown()

	dc := createTestDownstream(t, srv)
	registerDownstreamConn(t, dc, network)

	var ok bool
	uc, ok = acceptWithin(t, upstream, 2*time.Second)
	if !ok {
		t.Fatal("expected upstream connection")
	}
	registerUpstreamConn(t, uc)
	roundtrip(t, uc)
	roundtrip(t, dc)

	uc.WriteMessage(&irc.Message{
		Tags:    irc.Tags{"time": xirc.FormatServerTime(time.Now())},
		Prefix:  &irc.Prefix{Name: "alice"},
		Command: "PRIVMSG",
		Params:  []string{testUsername, "keep me"},
	})
	if _, ok := readUntilCommand(t, dc, "PRIVMSG", 2*time.Second); !ok {
		t.Fatal("did not receive PRIVMSG downstream")
	}

	// Disconnect client -> grace timer starts
	dc.WriteMessage(&irc.Message{Command: "QUIT"})
	expectClosed(t, dc, 2*time.Second)

	// Reconnect before grace expires
	dc2 := createTestDownstream(t, srv)
	defer dc2.Close()
	registerDownstreamConn(t, dc2, network)
	roundtrip(t, dc2)

	// Wait past original grace period; upstream must stay connected and messages preserved
	time.Sleep(1400 * time.Millisecond)

	dc2.WriteMessage(&irc.Message{
		Command: "CHATHISTORY",
		Params:  []string{"LATEST", "alice", "*", "10"},
	})
	var got []string
	for _, m := range roundtrip(t, dc2) {
		if m.Command == "PRIVMSG" {
			got = append(got, m.Params[1])
		}
	}
	if len(got) != 1 || got[0] != "keep me" {
		t.Fatalf("expected message preserved after grace cancellation, got %v", got)
	}
}

func TestEphemeral_SpecialQuitNow(t *testing.T) {
	db := createTempSqliteDB(t)
	user := createTestUser(t, db)
	network, upstream := createTestUpstream(t, db, user)
	defer upstream.Close()

	// Long grace timer so only /quit now can trigger immediate hibernation
	srv := newEphemeralTestServer(t, db, time.Hour, 24*time.Hour, "DEFAULT_QUIT")
	if err := srv.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer srv.Shutdown()

	// Connect two downstream clients (one network-specific, one bouncer)
	dc1 := createTestDownstream(t, srv)
	registerDownstreamConn(t, dc1, network)

	uc, ok := acceptWithin(t, upstream, 2*time.Second)
	if !ok {
		t.Fatal("expected upstream connection")
	}
	registerUpstreamConn(t, uc)
	roundtrip(t, uc)
	roundtrip(t, dc1)

	dc2 := createTestDownstream(t, srv)
	dc2.WriteMessage(&irc.Message{Command: "PASS", Params: []string{testPassword}})
	dc2.WriteMessage(&irc.Message{Command: "NICK", Params: []string{testUsername}})
	dc2.WriteMessage(&irc.Message{Command: "USER", Params: []string{testUsername, "0", "*", testUsername}})
	expectMessage(t, dc2, irc.RPL_WELCOME)
	roundtrip(t, dc2)

	// Send a message so msgstore has data
	uc.WriteMessage(&irc.Message{
		Tags:    irc.Tags{"time": xirc.FormatServerTime(time.Now())},
		Prefix:  &irc.Prefix{Name: "bob"},
		Command: "PRIVMSG",
		Params:  []string{testUsername, "wipe this"},
	})
	if _, ok := readUntilCommand(t, dc1, "PRIVMSG", 2*time.Second); !ok {
		t.Fatal("did not receive PRIVMSG on dc1")
	}

	// Issue /quit now from dc2 (the bouncer connection)
	dc2.WriteMessage(&irc.Message{
		Command: "QUIT",
		Params:  []string{"now"},
	})

	// Both downstreams must be closed
	expectClosed(t, dc2, 2*time.Second)
	expectClosed(t, dc1, 2*time.Second)

	// Upstream must receive QUIT with configured quit msg and close immediately
	quitMsg, ok := readUntilCommand(t, uc, "QUIT", 2*time.Second)
	if !ok {
		t.Fatal("expected upstream QUIT on /quit now")
	}
	if len(quitMsg.Params) != 1 || quitMsg.Params[0] != "DEFAULT_QUIT" {
		t.Fatalf("got QUIT params=%v, want [DEFAULT_QUIT]", quitMsg.Params)
	}
	expectClosed(t, uc, 2*time.Second)

	// Reconnect and verify messages were purged
	dc3 := createTestDownstream(t, srv)
	defer dc3.Close()
	registerDownstreamConn(t, dc3, network)
	uc2, ok := acceptWithin(t, upstream, 2*time.Second)
	if !ok {
		t.Fatal("expected upstream reconnect")
	}
	defer uc2.Close()
	registerUpstreamConn(t, uc2)
	roundtrip(t, dc3)

	dc3.WriteMessage(&irc.Message{
		Command: "CHATHISTORY",
		Params:  []string{"LATEST", "bob", "*", "10"},
	})
	for _, m := range roundtrip(t, dc3) {
		if m.Command == "PRIVMSG" {
			t.Fatalf("expected purged history after /quit now, got %v", m)
		}
	}
}

func TestEphemeral_QuitQueuedBeforeDownstreamClosed(t *testing.T) {
	for _, tc := range []struct {
		name    string
		reason  string
		quitMsg string
	}{
		{name: "now", reason: "now", quitMsg: "DEFAULT_QUIT"},
		{name: "now with message", reason: "now signal drop", quitMsg: "signal drop"},
		{name: "ordinary quit", reason: "later"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := createTempSqliteDB(t)
			record := createTestUser(t, db)
			network, upstream := createTestUpstream(t, db, record)
			defer upstream.Close()

			ctx := context.Background()
			store := msgstore.NewEphemeralStore(db)
			if _, err := store.Append(ctx, network, "bob", &irc.Message{
				Tags:    irc.Tags{"time": xirc.FormatServerTime(time.Now())},
				Prefix:  &irc.Prefix{Name: "bob"},
				Command: "PRIVMSG",
				Params:  []string{testUsername, "wipe this"},
			}); err != nil {
				t.Fatalf("Append failed: %v", err)
			}

			srv := newEphemeralTestServer(t, db, time.Hour, 24*time.Hour, "DEFAULT_QUIT")
			if err := srv.Start(); err != nil {
				t.Fatalf("failed to start server: %v", err)
			}
			defer srv.Shutdown()

			client := createTestDownstream(t, srv)
			defer client.Close()
			registerDownstreamConn(t, client, network)
			uc, ok := acceptWithin(t, upstream, 2*time.Second)
			if !ok {
				t.Fatal("expected upstream connection")
			}
			defer uc.Close()
			registerUpstreamConn(t, uc)
			roundtrip(t, uc)
			roundtrip(t, client)

			// Pause the user loop on the final help reply so the queued QUIT
			// is guaranteed to be handled after the downstream has closed.
			u := srv.getUser(testUsername)
			replies := make(chan userRunMsg)
			u.events <- eventUserRun{params: []string{"help"}, ch: replies}
			<-replies // help text; the final reply blocks until resume
			paused := true
			resume := func() {
				if paused {
					<-replies
					paused = false
				}
			}
			defer resume()

			dc := u.downstreamConns[0]
			u.events <- eventDownstreamMessage{dc: dc, msg: &irc.Message{
				Tags:    irc.Tags{"label": "19"},
				Command: "QUIT",
				Params:  []string{tc.reason},
			}}
			dc.Close()
			resume()

			if tc.quitMsg != "" {
				quit, ok := readUntilCommand(t, uc, "QUIT", 2*time.Second)
				if !ok {
					t.Fatal("expected immediate upstream QUIT after downstream closed")
				}
				if len(quit.Params) != 1 || quit.Params[0] != tc.quitMsg {
					t.Fatalf("got QUIT params=%v, want [%s]", quit.Params, tc.quitMsg)
				}
				expectClosed(t, uc, 2*time.Second)
			} else {
				roundtrip(t, uc) // ordinary QUIT must leave upstream connected
			}

			// Wait for the queued commands to finish before checking the DB.
			u.events <- eventUserRun{ch: replies}
			<-replies
			saved, err := db.GetUser(ctx, testUsername)
			if err != nil {
				t.Fatalf("GetUser failed: %v", err)
			}
			wantEnabled := tc.quitMsg == ""
			if saved.Enabled != wantEnabled {
				t.Fatalf("user enabled=%v, want %v", saved.Enabled, wantEnabled)
			}
			messages, err := store.LoadBeforeTime(ctx, time.Now().Add(time.Minute), time.Time{}, &msgstore.LoadMessageOptions{
				Network: network,
				Entity:  "bob",
				Limit:   10,
			})
			if err != nil {
				t.Fatalf("LoadBeforeTime failed: %v", err)
			}
			wantMessages := 0
			if wantEnabled {
				wantMessages = 1
			}
			if len(messages) != wantMessages {
				t.Fatalf("got %d messages, want %d", len(messages), wantMessages)
			}
		})
	}
}

func TestEphemeral_HistoryPeriodicExpiry(t *testing.T) {
	origInterval := ephemeralExpiryInterval
	ephemeralExpiryInterval = 50 * time.Millisecond
	defer func() { ephemeralExpiryInterval = origInterval }()

	db := createTempSqliteDB(t)
	user := createTestUser(t, db)
	network, upstream := createTestUpstream(t, db, user)
	defer upstream.Close()

	srv := newEphemeralTestServer(t, db, time.Hour, 5*time.Minute, "BYE")
	// Set a huge MessageExpiry to verify EphemeralHistory overrides it
	cfg := *srv.Config()
	cfg.MessageExpiry = 365 * 24 * time.Hour
	srv.SetConfig(&cfg)

	if err := srv.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	var uc ircConn
	t.Cleanup(func() {
		if uc != nil {
			uc.Close()
		}
	})
	defer srv.Shutdown()

	dc := createTestDownstream(t, srv)
	defer dc.Close()
	registerDownstreamConn(t, dc, network)

	var ok bool
	uc, ok = acceptWithin(t, upstream, 2*time.Second)
	if !ok {
		t.Fatal("expected upstream connection")
	}
	registerUpstreamConn(t, uc)
	roundtrip(t, uc)
	roundtrip(t, dc)

	// Send one old message (10m ago, > 5m ephemeral-history) and one recent message
	oldTime := time.Now().Add(-10 * time.Minute)
	newTime := time.Now()
	uc.WriteMessage(&irc.Message{
		Tags:    irc.Tags{"time": xirc.FormatServerTime(oldTime)},
		Prefix:  &irc.Prefix{Name: "alice"},
		Command: "PRIVMSG",
		Params:  []string{testUsername, "old message"},
	})
	uc.WriteMessage(&irc.Message{
		Tags:    irc.Tags{"time": xirc.FormatServerTime(newTime)},
		Prefix:  &irc.Prefix{Name: "alice"},
		Command: "PRIVMSG",
		Params:  []string{testUsername, "new message"},
	})
	roundtrip(t, uc)
	roundtrip(t, dc) // drain live-forwarded messages

	// Wait for periodic cleanup ticker to fire
	time.Sleep(150 * time.Millisecond)

	dc.WriteMessage(&irc.Message{
		Command: "CHATHISTORY",
		Params:  []string{"LATEST", "alice", "*", "10"},
	})
	var got []string
	for _, m := range roundtrip(t, dc) {
		if m.Command == "PRIVMSG" {
			got = append(got, m.Params[1])
		}
	}
	if len(got) != 1 || got[0] != "new message" {
		t.Fatalf("expected only new message after ephemeral-history expiry, got %v", got)
	}
}

func TestEphemeral_DisableInactiveUserIgnored(t *testing.T) {
	db := createTempSqliteDB(t)
	user := createTestUser(t, db)
	// Make user look inactive for 100 days
	user.DownstreamInteractedAt = time.Now().Add(-100 * 24 * time.Hour)
	if err := db.StoreUser(context.Background(), user); err != nil {
		t.Fatalf("StoreUser: %v", err)
	}

	srv := newEphemeralTestServer(t, db, time.Hour, 24*time.Hour, "BYE")
	cfg := *srv.Config()
	cfg.DisableInactiveUsersDelay = 24 * time.Hour
	srv.SetConfig(&cfg)

	if err := srv.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	defer srv.Shutdown()

	if err := srv.disableInactiveUsers(context.Background()); err != nil {
		t.Fatalf("disableInactiveUsers: %v", err)
	}

	got, err := db.GetUser(context.Background(), testUsername)
	if err != nil {
		t.Fatalf("GetUser: %v", err)
	}
	if !got.Enabled {
		t.Fatal("expected user to remain enabled when ephemeral-mode is active")
	}
}

var _ = io.EOF
