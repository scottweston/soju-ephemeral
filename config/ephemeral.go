package config

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"codeberg.org/emersion/soju/database"
	"codeberg.org/emersion/soju/msgstore"
)

const (
	defaultEphemeralGrace   = 15 * time.Minute
	defaultEphemeralHistory = 24 * time.Hour
)

func parseEphemeralDuration(s string) (time.Duration, error) {
	if strings.HasSuffix(s, "d") {
		return parseDuration(s)
	}
	dur, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("invalid duration: %v", err)
	}
	return dur, nil
}

func applyEphemeralConfig(srv *Server, ephemeralMode, quitMsg, grace, history string) error {
	if ephemeralMode != "" {
		b, err := strconv.ParseBool(ephemeralMode)
		if err != nil {
			return fmt.Errorf("directive ephemeral-mode: %v", err)
		}
		srv.EphemeralMode = b
	}

	srv.EphemeralQuitMsg = quitMsg

	if grace != "" {
		dur, err := parseEphemeralDuration(grace)
		if err != nil {
			return fmt.Errorf("directive ephemeral-grace: %v", err)
		} else if dur < 0 {
			return fmt.Errorf("directive ephemeral-grace: duration must be non-negative")
		}
		srv.EphemeralGrace = dur
	}

	if history != "" {
		dur, err := parseEphemeralDuration(history)
		if err != nil {
			return fmt.Errorf("directive ephemeral-history: %v", err)
		} else if dur < 0 {
			return fmt.Errorf("directive ephemeral-history: duration must be non-negative")
		}
		srv.EphemeralHistory = dur
	}

	if srv.MsgStore.Driver == msgstore.DriverEphemeral && srv.DB.Driver != database.DriverSQLite3 {
		return fmt.Errorf("directive message-store: driver %q only supports sqlite3 db", msgstore.DriverEphemeral)
	}

	if srv.EphemeralMode {
		switch srv.MsgStore.Driver {
		case msgstore.DriverDB, msgstore.DriverEphemeral:
			// ok
		default:
			return fmt.Errorf("directive ephemeral-mode: incompatible with message-store driver %q (supported drivers: db, ephemeral)", srv.MsgStore.Driver)
		}
	}

	return nil
}
