package emigrate

import (
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

type mockMigration struct {
	version int64 // the version of the migration
	err     error // an error to be returned as the result of Upgrade (or nil)
	called  bool  // true if the migrate script was called
}

func (mm *mockMigration) Version() int64 {
	return mm.version
}

func (mm *mockMigration) Upgrade(tx *sql.Tx) error {
	// upgrade is called, but may fail
	mm.called = true
	return mm.err
}

func setupVersioned(t *testing.T, currentVersion int64) (*sqlmock.MockDB, Migrator) {
	mock, db, err := sqlmock.New()
	if err != nil {
		t.Errorf("Unexpected error '%s' while opening mock db connection", err)
	}
	// Set the current version
	result := fmt.Sprintf("%d", currentVersion)
	mock.ExpectQuery(QueryGetCurrentVersion).
		WillReturnRows(sqlmock.NewRows([]string{"version"}).FromCSVString(result))
	return mock, Migrator{db: db}
}

func TestFailingToGetCurrentVersion(t *testing.T) {
	t.Parallel()
	mock, db, err := sqlmock.New()
	if err != nil {
		t.Errorf("Unexpected error '%s' while opening mock db connection", err)
	}

	dbErr := errors.New("db failed")
	mock.ExpectQuery(QueryGetCurrentVersion).
		WillReturnError(dbErr)
	m := Migrator{db: db}

	if _, result := m.UpgradeToVersion(99); result != dbErr {
		t.Errorf("Expected %v, got %v", dbErr, result)
	}
	mock.CloseTest(t)
}

func TestDowngradesUnsupported(t *testing.T) {
	t.Parallel()
	mock, m := setupVersioned(t, 99)

	expected := DowngradesUnsupported
	if _, result := m.UpgradeToVersion(1); result != expected {
		t.Errorf("Expected %v, got %v", expected, result)
	}
	mock.CloseTest(t)
}

func TestDBAtRequestedVersion(t *testing.T) {
	t.Parallel()
	mock, m := setupVersioned(t, 99)

	if _, result := m.UpgradeToVersion(99); result != nil {
		t.Errorf("Expected %v, got %v", nil, result)
	}
	mock.CloseTest(t)
}

func TestMissingCurrentMigration(t *testing.T) {
	t.Parallel()
	mock, m := setupVersioned(t, 2)

	// second migration is missing
	m.migrations = migrationRange(1, 3)
	expected := MissingCurrentMigration
	if _, result := m.UpgradeToVersion(3); result != expected {
		t.Errorf("Expected %v, got %v", expected, result)
	}
	mock.CloseTest(t)
}

func TestFutureMigrationsApplied(t *testing.T) {
	t.Parallel()
	mock, m := setupVersioned(t, 2)
	m.migrations = migrationRange(1, 2, 3, 4)

	expectSetVersions(2, mock, 3, 4)
	_, err := m.UpgradeToVersion(4)
	if err != nil {
		t.Fatalf("Unexpected error during migration: %s", err.Error())
	}

	// Only 3 and 4 should be applied
	expected := []bool{false, false, true, true}
	for idx, val := range expected {
		result := m.migrations[idx].(*mockMigration).called
		version := m.migrations[idx].Version()
		if result != val {
			t.Fatalf("Version %d application mismatch: expected %v, got %v", version, val, result)
		}
	}
	mock.CloseTest(t)
}

func TestFutureMigrationsAppliedAutomatic(t *testing.T) {
	t.Parallel()
	mock, m := setupVersioned(t, 2)
	m.migrations = migrationRange(1, 2, 3, 4)

	expectSetVersions(2, mock, 3, 4)
	_, err := m.Upgrade()
	if err != nil {
		t.Fatalf("Unexpected error during migration: %s", err.Error())
	}

	// Only 3 and 4 should be applied
	expected := []bool{false, false, true, true}
	for idx, val := range expected {
		result := m.migrations[idx].(*mockMigration).called
		version := m.migrations[idx].Version()
		if result != val {
			t.Fatalf("Version %d application mismatch: expected %v, got %v", version, val, result)
		}
	}
	mock.CloseTest(t)
}

func TestMigrationStopsIfBeginFails(t *testing.T) {
	t.Parallel()
	mock, m := setupVersioned(t, 1)
	m.migrations = migrationRange(1, 2, 3)

	dbErr := errors.New("begin failed")
	mock.ExpectBegin().WillReturnError(dbErr)

	_, result := m.UpgradeToVersion(2)
	if result != dbErr {
		t.Errorf("Expected %v, got %v", dbErr, result)
	}
	mock.CloseTest(t)
}

func TestFailedMigrationHalts(t *testing.T) {
	t.Parallel()
	mock, m := setupVersioned(t, 1)
	m.migrations = migrationRange(1, 2, 3)
	mock.ExpectBegin()
	expectVersionQuery(mock, 1)

	expected := errors.New("migrate failed")
	m.migrations[1].(*mockMigration).err = expected

	_, result := m.UpgradeToVersion(3)
	if result != expected {
		t.Errorf("Expected %v, got %v", expected, result)
	}
	if m.migrations[2].(*mockMigration).called {
		t.Errorf("Migration called when it shouldn't have been")
	}
	mock.CloseTest(t)
}

// Returns a slice of migrations at set version numbers, in the order
// specified
func migrationRange(versions ...int64) []Migration {
	ms := make([]Migration, len(versions))
	for idx, version := range versions {
		ms[idx] = &mockMigration{version: version}
	}
	return ms
}

// Sets up the database mock to expect a set of version updates
func expectSetVersions(current int64, mock *sqlmock.MockDB, versions ...int64) {
	// We don't use prepared statements, but could check here if we did
	for _, version := range versions {
		mock.ExpectBegin()
		expectVersionQuery(mock, current)
		statement := QuerySetVersion(version)
		mock.ExpectExec(statement).
			WillReturnResult(sqlmock.NewResult(0, 1))
		current = version
		mock.ExpectCommit()
	}
}

func expectVersionQuery(mock *sqlmock.MockDB, version int64) {
	mock.ExpectQuery(QueryGetCurrentVersion).
		WillReturnRows(sqlmock.NewRows([]string{"version"}).
		FromCSVString(fmt.Sprintf("%d", version)))
}
