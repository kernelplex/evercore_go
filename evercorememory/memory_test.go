package evercore_test

import (
	"testing"

	"github.com/kernelplex/evercore_go/evercore"
	"github.com/kernelplex/evercore_go/evercoreenginetests"
)

func TestMemoryStorage(t *testing.T) {
	storageEngine := evercore.NewMemoryStorageEngine()
	testSuite := evercoreenginetests.NewStorageEngineTestSuite(storageEngine)
	testSuite.RunTests(t)
}
