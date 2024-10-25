package evercorepostgres_test

import (
	"testing"

	"github.com/kernelplex/evercore_go/evercore"
	"github.com/kernelplex/evercore_go/evercorepostgres"
)

// Ensure the PostgresStorageEngine implements StorageEngine
func testPostgresStorageEngine_ImplementsStorageEngine(_ *testing.T) {
	storageEngine := &evercorepostgres.PostgresStorageEngine{}
	var _ evercore.StorageEngine = storageEngine
}
