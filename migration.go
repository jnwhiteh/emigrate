package emigrate

import (
	"database/sql"
	"errors"
	"fmt"
	"sort"
)

// Errors that could be returned
var (
	MissingCurrentMigration = errors.New("Cannot find current migration")
	DowngradesUnsupported   = errors.New("Downgrades are not currently supported")
	MigrationVersionChanged = errors.New("Current migration version changed")
	InitVersionMismatch     = errors.New("Migration version mismatch during init")
)

// Queries that might be executed by emigrate
var (
	QueryGetCurrentVersion = `SELECT version FROM emigrate LIMIT 1`
	QuerySetVersion        = func(version int64) string {
		return fmt.Sprintf(`UPDATE migration SET version = %d`, version)
	}
	QueryCreateTable = `CREATE TABLE emigrate (version INTEGER)`
)

type Migration interface {
	Version() int64
	Upgrade(db *sql.Tx) error
}

// byVersion implements sorting a migration list by version
type byVersion []Migration

func (a byVersion) Len() int           { return len(a) }
func (a byVersion) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a byVersion) Less(i, j int) bool { return a[i].Version() < a[j].Version() }
func (a byVersion) Search(version int64) (int, bool) {
	idx := sort.Search(len(a), func(i int) bool {
		return a[i].Version() >= version
	})
	if idx < len(a) && a[idx].Version() == version {
		return idx, true
	} else {
		return idx, false
	}
}

type Migrator struct {
	db         *sql.DB     // the database on which to perform the migrations
	migrations []Migration // a list of migrations
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

func (m *Migrator) setVersion(tx *sql.Tx, version int64) error {
	query := QuerySetVersion(version)
	_, err := tx.Exec(query)
	return err
}

// Migration currently only supports upgrades
func (m *Migrator) Migrate(version int64) error {
	current, err := m.CurrentVersion()
	if err != nil {
		return err
	} else if version < current {
		return DowngradesUnsupported
	} else if current == version {
		return nil
	}

	// sort the list of migrations
	sort.Sort(byVersion(m.migrations))

	// get the list of migrations to apply
	migrations := m.migrations
	if current > 0 {
		idx, ok := byVersion(m.migrations).Search(current)
		if !ok {
			return MissingCurrentMigration
		}
		migrations = migrations[idx+1:]
	}

	// apply each migration in turn, stopping when an error occurs
	for _, migration := range migrations {
		err = m.apply(migration)
		if err != nil {
			return err
		}
	}

	return nil
}

func (m *Migrator) apply(migration Migration) error {
	// new transaction
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}

	// verify we're still on the correct version
	current, err := m.CurrentVersion()
	if err != nil {
		return err
	} else if current != migration.Version()-1 {
		return MigrationVersionChanged
	}

	// apply the migration
	err = migration.Upgrade(tx)
	if err != nil {
		tx.Rollback()
		return err
	}

	// update the migration version
	current = migration.Version()
	err = m.setVersion(tx, current)
	if err != nil {
		tx.Rollback()
		return err
	}
	// commit
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
	tx, err := m.db.Begin()
	if err != nil {
		return err
	}

	current, err := m.CurrentVersion()
	if err == nil {
		// this database is already versioned
		return nil
	}

	// try to create the emigrate table
	_, err = tx.Exec(QueryCreateTable)
	if err != nil {
		return err
	}
	err = m.setVersion(tx, 0)
	if err != nil {
		return err
	}

	// hope for the best!
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
