package evercorememory_test

import (
	"testing"

	"github.com/kernelplex/evercore_go/evercoreenginetests"
	"github.com/kernelplex/evercore_go/evercorememory"
)

func TestMemoryStorage(t *testing.T) {
	storageEngine := evercorememory.NewMemoryStorageEngine()
	testSuite := evercoreenginetests.NewStorageEngineTestSuite(storageEngine)
	testSuite.RunTests(t)
}
