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
		return fmt.Sprintf(`UPDATE emigrate SET version = %d`, version)
	}
	QueryCreateTable   = `CREATE TABLE emigrate (version INTEGER)`
	QueryInsertVersion = `INSERT INTO emigrate (version) VALUES (0)`
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
