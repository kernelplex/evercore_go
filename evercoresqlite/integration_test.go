//go:build integration

package evercoresqlite_test

import (
	"database/sql"
	"testing"

	"github.com/kernelplex/evercore_go/evercoreenginetests"
	"github.com/kernelplex/evercore_go/evercoresqlite"
	_ "github.com/mattn/go-sqlite3"
)

func TestSqliteDatastore(t *testing.T) {

	db, err := sql.Open("sqlite3", "file::memory:?cache=shared")
	if err != nil {
		panic(err)
	}
	evercoresqlite.MigrateUp(db)

	iut := evercoresqlite.NewSqliteStorageEngine(db)
	testSuite := sharedintegrationtests.NewStorageEngineTestSuite(iut)
	testSuite.RunTests(t)
}

func TestNewSqliteStorageEngineWithConnection(t *testing.T) {
	_, err := evercoresqlite.NewSqliteStorageEngineWithConnection("file::memory:?cache=shared")
	if err != nil {
		t.Errorf("NewSqliteStorageEngineWithConnection failed: %s", err)
	}
}
