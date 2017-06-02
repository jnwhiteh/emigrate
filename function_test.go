package emigrate

import (
	"database/sql"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
)

func TestVersionFunctionMigration(t *testing.T) {
	var expected int64 = 1
	m := functionMigration{expected, nil, nil}

	result := m.Version()
	if result != expected {
		t.Errorf("Expected %d, got %d", expected, result)
	}
}

// Verify that a function migration's upgrade script is run when trhe
// migration is applied.
func TestUpgradeFunctionMigration(t *testing.T) {
	mock, m := setupVersioned(t, 0)
	v1 := &functionMigration{
		1,
		func(tx *sql.Tx) error {
			_, err := tx.Exec(TestQueryCreateInvoiceTable)
			return err
		},
		func(tx *sql.Tx) error {
			_, err := tx.Exec(TestQueryDropInvoiceTable)
			return err
		},
	}
	m.migrations = append(m.migrations, v1)

	mock.ExpectBegin()
	expectVersionQuery(mock, 0)
	mock.ExpectExec(regexp.QuoteMeta(TestQueryCreateInvoiceTable)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec(QuerySetVersion(1)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	err := m.Migrate(1)
	if err != nil {
		t.Fatalf("Error during migration: %s", err)
	}
	mock.CloseTest(t)
}
