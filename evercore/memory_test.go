package evercore_test

import (
	"testing"

	"github.com/kernelplex/evercore_go/enginetests"
	"github.com/kernelplex/evercore_go/evercore"
)

func TestMemoryStorage(t *testing.T) {
	storageEngine := evercore.NewMemoryStorageEngine()
	testSuite := sharedintegrationtests.NewStorageEngineTestSuite(storageEngine)
	testSuite.RunTests(t)
}
