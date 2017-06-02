package emigrate

import "github.com/DATA-DOG/go-sqlmock"
import "testing"
import "regexp"

var (
	TestQueryCreateInvoiceTable = `CREATE TABLE "invoice" (id INTEGER, sold BOOLEAN)`
	TestQueryDropInvoiceTable   = `DROP TABLE "invoice"`
)

func TestVersionStringMigration(t *testing.T) {
	var expected int64 = 1
	m := stringMigration{expected, "", ""}

	result := m.Version()
	if result != expected {
		t.Errorf("Expected %d, got %d", expected, result)
	}
}

// Verify that a string migration's upgrade script is run when the migration
// is applied.
func TestUpgradeStringMigration(t *testing.T) {
	mock, m := setupVersioned(t, 0)
	v1 := stringMigration{1, TestQueryCreateInvoiceTable, TestQueryDropInvoiceTable}
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
