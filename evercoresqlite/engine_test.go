package evercoresqlite_test

import (
	"testing"

	"github.com/kernelplex/evercore_go/evercore"
	"github.com/kernelplex/evercore_go/evercoresqlite"
)

// Ensure the SqliteStorageEngine implements StorageEngine
func testSqliteStorageEngine_ImplementsStorageEngine(_ *testing.T) {
	var _ evercore.StorageEngine = &evercoresqlite.SqliteStorageEngine{}
}
