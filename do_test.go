package batching_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/golang-one/batching"
)

func TestDo(t *testing.T) {
	defer goleak.VerifyNone(t)

	t.Run("min size for concurrency not reached", func(t *testing.T) {
		ctx := context.Background()
		array := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
		var batches [][]int
		err := batching.Do(ctx, array, func(ctx context.Context, batch []int) error {
			batches = append(batches, batch)
			return nil
		}, batching.BatchSize(1), batching.MaxThreads(1), batching.MinSizeForConcurrency(100))

		require.NoError(t, err)
		require.Equal(t, [][]int{{1, 2, 3, 4, 5, 6, 7, 8, 9}}, batches)
	})

	t.Run("single threaded batch by 1", func(t *testing.T) {
		ctx := context.Background()
		array := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
		var batches [][]int
		err := batching.Do(ctx, array, func(ctx context.Context, batch []int) error {
			batches = append(batches, batch)
			return nil
		}, batching.BatchSize(1), batching.MaxThreads(1), batching.MinSizeForConcurrency(5))

		require.NoError(t, err)
		require.Equal(t, [][]int{{1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}}, batches)
	})

	t.Run("single threaded batch by 3", func(t *testing.T) {
		ctx := context.Background()
		array := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}
		var batches [][]int
		err := batching.Do(ctx, array, func(ctx context.Context, batch []int) error {
			batches = append(batches, batch)
			return nil
		}, batching.BatchSize(3), batching.MaxThreads(1), batching.MinSizeForConcurrency(5))

		require.NoError(t, err)
		require.Equal(t, [][]int{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}, {10, 11}}, batches)
	})

	t.Run("multi threaded batch by 1", func(t *testing.T) {
		ctx := context.Background()
		array := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}

		var (
			batches [][]int
			mx      sync.Mutex
		)
		err := batching.Do(ctx, array, func(ctx context.Context, batch []int) error {
			mx.Lock()
			defer mx.Unlock()
			batches = append(batches, batch)
			return nil
		}, batching.BatchSize(1), batching.MaxThreads(100), batching.MinSizeForConcurrency(5))

		require.NoError(t, err)
		require.ElementsMatch(t, [][]int{{1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}}, batches)
	})

	t.Run("multi threaded batch by 3", func(t *testing.T) {
		ctx := context.Background()
		array := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}

		var (
			batches [][]int
			mx      sync.Mutex
		)
		err := batching.Do(ctx, array, func(ctx context.Context, batch []int) error {
			mx.Lock()
			defer mx.Unlock()
			batches = append(batches, batch)
			return nil
		}, batching.BatchSize(3), batching.MaxThreads(100), batching.MinSizeForConcurrency(5))

		require.NoError(t, err)
		require.ElementsMatch(t, [][]int{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}, {10, 11}}, batches)
	})

	t.Run("cancel context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		array := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
		var batches [][]int
		err := batching.Do(ctx, array, func(ctx context.Context, batch []int) error {
			// NOTE: second iteration may acquire semaphore and proceed with cancelled context.
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}

			batches = append(batches, batch)
			cancel()
			return nil
		}, batching.BatchSize(4), batching.MaxThreads(1), batching.MinSizeForConcurrency(5))

		require.Error(t, err)
		require.Equal(t, [][]int{{1, 2, 3, 4}}, batches)
	})

	t.Run("return error", func(t *testing.T) {
		ctx := context.Background()
		array := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
		var batches [][]int
		err := batching.Do(ctx, array, func(ctx context.Context, batch []int) error {
			batches = append(batches, batch)
			return errors.New("intended")
		}, batching.BatchSize(4), batching.MaxThreads(1), batching.MinSizeForConcurrency(5))

		require.Error(t, err)
		require.Equal(t, [][]int{{1, 2, 3, 4}}, batches)
	})

	t.Run("return error in last batch", func(t *testing.T) {
		ctx := context.Background()
		array := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
		var batches [][]int
		err := batching.Do(ctx, array, func(ctx context.Context, batch []int) error {
			batches = append(batches, batch)
			return errors.New("intended")
		}, batching.BatchSize(10), batching.MaxThreads(1), batching.MinSizeForConcurrency(5))

		require.Error(t, err)
		require.Equal(t, [][]int{{1, 2, 3, 4, 5, 6, 7, 8, 9}}, batches)
	})

	t.Run("panic", func(t *testing.T) {
		ctx := context.Background()
		array := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
		var batches [][]int

		require.PanicsWithValue(t, "intended", func() {
			_ = batching.Do(ctx, array, func(ctx context.Context, batch []int) error {
				batches = append(batches, batch)
				panic("intended")
			}, batching.BatchSize(4), batching.MaxThreads(1), batching.MinSizeForConcurrency(5))
		})

		require.Equal(t, [][]int{{1, 2, 3, 4}}, batches)
	})

	t.Run("panic in last batch", func(t *testing.T) {
		ctx := context.Background()
		array := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
		var batches [][]int

		require.PanicsWithValue(t, "intended", func() {
			_ = batching.Do(ctx, array, func(ctx context.Context, batch []int) error {
				batches = append(batches, batch)
				panic("intended")
			}, batching.BatchSize(10), batching.MaxThreads(1), batching.MinSizeForConcurrency(5))
		})

		require.Equal(t, [][]int{{1, 2, 3, 4, 5, 6, 7, 8, 9}}, batches)
	})
}
