package emigrate

import (
	"database/sql"
	"fmt"
)

// stringMigration is an implementation of Migration that supports upgrading
// and downgrading based on SQL statements stored in strings
type stringMigration struct {
	version int64  // the version number for this migration
	up      string // the string to run when upgrading
	down    string // the string to run when downgrading
}

func NewStringMigration(version int64, up, down string) Migration {
	return &stringMigration{version, up, down}
}

func (m stringMigration) Version() int64 {
	return m.version
}

func (m stringMigration) Upgrade(tx *sql.Tx) error {
	_, err := tx.Exec(m.up)
	return err
}

func (m stringMigration) Downgrade(tx *sql.Tx) error {
	if m.down == "" {
		return fmt.Errorf("emigrate: No downgrade defined for migration %d", m.version)
	}
	_, err := tx.Exec(m.down)
	return err
}
