package emigrate

import (
	"database/sql"
	"fmt"
	"sort"
)

type Migrator struct {
	db         *sql.DB     // the database on which to perform the migrations
	migrations []Migration // a list of migrations
}

func NewMigrator(db *sql.DB, migrations []Migration) *Migrator {
	return &Migrator{db, migrations}
}

// CurrentVersion returns the current migration version of the database
func (m *Migrator) CurrentVersion() (int64, error) {
	var currentVersion int64
	err := m.db.QueryRow(QueryGetCurrentVersion).Scan(&currentVersion)
	if err != nil {
		return 0, err
	}
	return currentVersion, err
}

func (m *Migrator) MaxVersion() int64 {
	var max int64 = 0
	for _, migration := range m.migrations {
		if migration.Version() >= max {
			max = migration.Version()
		}
	}
	return max
}

func (m *Migrator) setVersion(tx *sql.Tx, version int64) error {
	query := QuerySetVersion(version)
	_, err := tx.Exec(query)
	return err
}

func (m *Migrator) Upgrade() ([]string, error) {
	maxVersion := m.MaxVersion()
	return m.UpgradeToVersion(maxVersion)
}

// Migration currently only supports upgrades
func (m *Migrator) UpgradeToVersion(version int64) ([]string, error) {
	current, err := m.CurrentVersion()
	if err != nil {
		return nil, err
	} else if version < current {
		return nil, DowngradesUnsupported
	} else if current == version {
		message := "emigrate: database already at current version"
		return []string{message}, nil
	}

	sort.Sort(byVersion(m.migrations))

	migrations := m.migrations
	if current > 0 {
		idx, ok := byVersion(m.migrations).Search(current)
		if !ok {
			return nil, MissingCurrentMigration
		}
		migrations = migrations[idx+1:]
	}

	var log []string
	for _, migration := range migrations {
		err = m.apply(migration)
		if err != nil {
			return nil, err
		}
		log = append(log, fmt.Sprintf("emigrate: upgraded to version %d", migration.Version()))
	}

	return log, nil
}

func (m *Migrator) apply(migration Migration) error {

	tx, err := m.db.Begin()
	if err != nil {
		return err
	}

	current, err := m.CurrentVersion()
	if err != nil {
		return err
	} else if current != migration.Version()-1 {
		return MigrationVersionChanged
	}

	err = migration.Upgrade(tx)
	if err != nil {
		tx.Rollback()
		return err
	}

	current = migration.Version()
	err = m.setVersion(tx, current)
	if err != nil {
		tx.Rollback()
		return err
	}

	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		return err
	}
	return nil
}

// Init ensures that the database is properly initialized to be managed by
// emigrate. If the emigrate tables do not exist they are created.
func (m *Migrator) Init() error {
	// try to get the current version, may fail if table doesn't exist
	current, err := m.CurrentVersion()
	if err == nil {
		return nil
	}

	// try to create the emigrate table
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}

	_, err = tx.Exec(QueryCreateTable)
	if err != nil {
		return err
	}
	_, err = tx.Exec(QueryInsertVersion)
	if err != nil {
		return err
	}
	err = tx.Commit()
	if err != nil {
		return err
	}

	current, err = m.CurrentVersion()
	if err != nil {
		return err
	} else if current != 0 {
		return InitVersionMismatch
	}

	return nil
}
