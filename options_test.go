package batching

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDoOptions(t *testing.T) {
	t.Run("batchSize", func(t *testing.T) {
		doOpts := doOptions{
			batchSize: -1,
		}

		require.Equal(t, BatchSize(-1).do(doOpts).batchSize, -1)
		require.Equal(t, BatchSize(0).do(doOpts).batchSize, -1)
		require.Equal(t, BatchSize(1).do(doOpts).batchSize, 1)
		require.Equal(t, BatchSize(2).do(doOpts).batchSize, 2)
		require.Equal(t, BatchSize(3).do(doOpts).batchSize, 3)
	})

	t.Run("minSizeForConcurrency", func(t *testing.T) {
		doOpts := doOptions{
			minSizeForConcurrency: -1,
		}

		require.Equal(t, MinSizeForConcurrency(-1).do(doOpts).minSizeForConcurrency, -1)
		require.Equal(t, MinSizeForConcurrency(0).do(doOpts).minSizeForConcurrency, -1)
		require.Equal(t, MinSizeForConcurrency(1).do(doOpts).minSizeForConcurrency, -1)
		require.Equal(t, MinSizeForConcurrency(2).do(doOpts).minSizeForConcurrency, 2)
		require.Equal(t, MinSizeForConcurrency(3).do(doOpts).minSizeForConcurrency, 3)
	})

	t.Run("maxThreads", func(t *testing.T) {
		doOpts := doOptions{
			maxThreads: -1,
		}

		require.Equal(t, MaxThreads(-1).do(doOpts).maxThreads, -1)
		require.Equal(t, MaxThreads(0).do(doOpts).maxThreads, -1)
		require.Equal(t, MaxThreads(1).do(doOpts).maxThreads, 1)
		require.Equal(t, MaxThreads(2).do(doOpts).maxThreads, 2)
		require.Equal(t, MaxThreads(3).do(doOpts).maxThreads, 3)
	})
}
