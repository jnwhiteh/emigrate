package emigrate

import "database/sql"

// functionMigration is an implementaiton of Migration that performs all
// upgrade and downgrade actions with Go functions.
type functionMigration struct {
	version int64                  // the version number of the migration
	up      func(tx *sql.Tx) error // the function to run on upgrade
	down    func(tx *sql.Tx) error // the function to run on downgrade
}

func (m *functionMigration) Version() int64 {
	return m.version
}

func (m *functionMigration) Upgrade(tx *sql.Tx) error {
	return m.up(tx)
}

func (m *functionMigration) Downgrade(tx *sql.Tx) error {
	return m.down(tx)
}
