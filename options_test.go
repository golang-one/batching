package batching

import (
	"reflect"
	"testing"
)

func TestDoOptions(t *testing.T) {
	t.Run("batchSize", func(t *testing.T) {
		doOpts := doOptions{
			batchSize: -1,
		}

		requireEqual(t, -1, BatchSize(-1).do(doOpts).batchSize)
		requireEqual(t, -1, BatchSize(0).do(doOpts).batchSize)
		requireEqual(t, 1, BatchSize(1).do(doOpts).batchSize)
		requireEqual(t, 2, BatchSize(2).do(doOpts).batchSize)
		requireEqual(t, 3, BatchSize(3).do(doOpts).batchSize)
	})

	t.Run("minSizeForConcurrency", func(t *testing.T) {
		doOpts := doOptions{
			minSizeForConcurrency: -1,
		}

		requireEqual(t, -1, MinSizeForConcurrency(-1).do(doOpts).minSizeForConcurrency)
		requireEqual(t, -1, MinSizeForConcurrency(0).do(doOpts).minSizeForConcurrency)
		requireEqual(t, -1, MinSizeForConcurrency(1).do(doOpts).minSizeForConcurrency)
		requireEqual(t, 2, MinSizeForConcurrency(2).do(doOpts).minSizeForConcurrency)
		requireEqual(t, 3, MinSizeForConcurrency(3).do(doOpts).minSizeForConcurrency)
	})

	t.Run("maxThreads", func(t *testing.T) {
		doOpts := doOptions{
			maxThreads: -1,
		}

		requireEqual(t, -1, MaxThreads(-1).do(doOpts).maxThreads)
		requireEqual(t, -1, MaxThreads(0).do(doOpts).maxThreads)
		requireEqual(t, 1, MaxThreads(1).do(doOpts).maxThreads)
		requireEqual(t, 2, MaxThreads(2).do(doOpts).maxThreads)
		requireEqual(t, 3, MaxThreads(3).do(doOpts).maxThreads)
	})
}

func requireEqual(t *testing.T, expected, actual any) {
	t.Helper()

	if !reflect.DeepEqual(expected, actual) {
		t.Fatalf("Not equal: %#+v != %#+v\n", expected, actual)
	}
}
