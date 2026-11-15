package soju

import (
	"context"
	"strings"
	"time"

	"gopkg.in/irc.v4"

	"codeberg.org/emersion/soju/database"
	"codeberg.org/emersion/soju/msgstore"
)

var (
	ephemeralExpiryInterval   = time.Minute
	ephemeralQuitFlushTimeout = 2 * time.Second
)

type eventEphemeralHibernate struct {
	gen uint64
}

type ephemeralUserState struct {
	active     bool
	graceTimer *time.Timer
	graceGen   uint64
}

func (e *ephemeralUserState) stopTimer() {
	e.graceGen++
	if e.graceTimer != nil {
		e.graceTimer.Stop()
		e.graceTimer = nil
	}
}

func (s *Server) ephemeralHistoryLoop() {
	ticker := time.NewTicker(ephemeralExpiryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.stopCh:
			return
		case <-ticker.C:
		}

		if err := s.deleteEphemeralExpiredMessages(context.TODO()); err != nil {
			s.Logger.Printf("failed to delete expired ephemeral messages: %v", err)
		}
	}
}

func (s *Server) deleteEphemeralExpiredMessages(ctx context.Context) error {
	cfg := s.Config()
	if !cfg.EphemeralMode || cfg.EphemeralHistory <= 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()

	before := time.Now().Add(-cfg.EphemeralHistory)

	switch cfg.MsgStore.Driver {
	case msgstore.DriverDB, msgstore.DriverEphemeral:
		if err := s.db.DeleteMessagesBefore(ctx, before); err != nil {
			return err
		}
	}

	return nil
}

func (u *user) startNetwork(net *network) {
	if u.srv.Config().EphemeralMode && !u.ephemeral.active {
		return
	}
	if net.started.Swap(true) {
		return
	}
	go net.run()
}

func (u *user) ephemeralOnDownstreamConnected(ctx context.Context) {
	u.ephemeral.stopTimer()

	if !u.srv.Config().EphemeralMode {
		return
	}

	u.ephemeral.active = true
	for _, net := range u.networks {
		u.startNetwork(net)
	}
}

func (u *user) ephemeralOnDownstreamDisconnected(ctx context.Context) {
	if !u.srv.Config().EphemeralMode {
		return
	}
	if len(u.downstreamConns) > 0 {
		return
	}
	if !u.ephemeral.active && !u.Enabled {
		return
	}

	grace := u.srv.Config().EphemeralGrace
	if grace <= 0 {
		u.ephemeralHibernate("")
		return
	}

	u.ephemeral.stopTimer()
	gen := u.ephemeral.graceGen
	u.logger.Printf("all downstream clients disconnected, hibernating in %v", grace)
	u.ephemeral.graceTimer = time.AfterFunc(grace, func() {
		select {
		case u.events <- eventEphemeralHibernate{gen: gen}:
		case <-u.done:
		}
	})
}

func (u *user) handleEphemeralHibernateEvent(e eventEphemeralHibernate) {
	if !u.srv.Config().EphemeralMode {
		return
	}
	if e.gen != u.ephemeral.graceGen || len(u.downstreamConns) > 0 {
		return
	}
	u.ephemeralHibernate("")
}

func (u *user) ephemeralHibernate(quitMsg string) {
	u.ephemeral.stopTimer()
	u.ephemeral.active = false

	effectiveQuitMsg := quitMsg
	if effectiveQuitMsg == "" {
		effectiveQuitMsg = u.srv.Config().EphemeralQuitMsg
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var closedChs []<-chan struct{}
	quitCtx, quitCancel := context.WithTimeout(ctx, ephemeralQuitFlushTimeout)
	for _, net := range u.networks {
		net.ephemeralStopped.Store(true)
		uc := net.conn
		if uc == nil || uc.isClosed() {
			continue
		}
		msg := &irc.Message{Command: "QUIT"}
		if effectiveQuitMsg != "" {
			msg.Params = []string{effectiveQuitMsg}
		}
		uc.SendMessage(quitCtx, msg)
		uc.Shutdown(quitCtx)
		closedChs = append(closedChs, uc.closedCh)
	}
	for _, ch := range closedChs {
		select {
		case <-ch:
		case <-quitCtx.Done():
		case <-u.srv.stopCh:
		}
	}
	quitCancel()

	if u.Enabled {
		if err := u.updateUser(ctx, func(record *database.User) error {
			record.Enabled = false
			return nil
		}); err != nil {
			u.logger.Printf("failed to hibernate user: %v", err)
		}
	} else {
		for _, dc := range u.downstreamConns {
			dc.Close()
		}
		for _, net := range u.networks {
			net.stop()
			if net.conn != nil {
				u.handleUpstreamDisconnected(ctx, net.conn)
			}
		}
	}

	u.purgeEphemeralMessages(ctx)
	u.logger.Printf("hibernated user and purged ephemeral messages")
}

func (u *user) purgeEphemeralMessages(ctx context.Context) {
	for _, net := range u.networks {
		net.delivered = newDeliveredStore(net.casemap)
	}

	if es, ok := u.msgStore.(msgstore.EphemeralStore); ok {
		if err := es.PurgeUser(ctx, u.ID); err != nil {
			u.logger.Printf("failed to purge ephemeral messages: %v", err)
		}
		return
	}

	if edb, ok := u.srv.db.(database.EphemeralDatabase); ok {
		if err := edb.DeleteUserMessages(ctx, u.ID); err != nil {
			u.logger.Printf("failed to purge messages from database: %v", err)
		}
	}
}

func (dc *downstreamConn) handleEphemeralQuit(ctx context.Context, msg *irc.Message) bool {
	if !dc.registered || dc.user == nil || !dc.srv.Config().EphemeralMode {
		return false
	}
	// Several downstreams may have queued QUIT before the first one hibernated us.
	if !dc.user.ephemeral.active {
		return false
	}
	if len(msg.Params) == 0 {
		return false
	}

	arg := strings.TrimSpace(msg.Params[0])
	cmd, rest, hasRest := strings.Cut(arg, " ")
	if !strings.EqualFold(cmd, "now") {
		return false
	}

	quitMsg := ""
	if hasRest {
		quitMsg = strings.TrimSpace(rest)
	}

	dc.conn.Shutdown(ctx)
	dc.user.ephemeralHibernate(quitMsg)
	return true
}
