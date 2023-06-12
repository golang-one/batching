package batching_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/golang-one/batching"
)

func TestPipe(t *testing.T) {
	defer goleak.VerifyNone(t)

	plus10 := func(ctx context.Context, batch []int) ([]int, error) {
		res := make([]int, len(batch))
		for i, v := range batch {
			res[i] = v + 10
		}
		return res, nil
	}

	t.Run("min size for concurrency not reached", func(t *testing.T) {
		ctx := context.Background()
		array := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
		res, err := batching.Pipe(ctx, array, plus10,
			batching.BatchSize(1), batching.MaxThreads(1), batching.MinSizeForConcurrency(100))

		require.NoError(t, err)
		require.Equal(t, []int{11, 12, 13, 14, 15, 16, 17, 18, 19}, res)
	})

	t.Run("single threaded batch by 1", func(t *testing.T) {
		ctx := context.Background()
		array := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
		res, err := batching.Pipe(ctx, array, plus10,
			batching.BatchSize(1), batching.MaxThreads(1), batching.MinSizeForConcurrency(5))

		require.NoError(t, err)
		require.Equal(t, []int{11, 12, 13, 14, 15, 16, 17, 18, 19}, res)
	})

	t.Run("single threaded batch by 3", func(t *testing.T) {
		ctx := context.Background()
		array := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}
		res, err := batching.Pipe(ctx, array, plus10,
			batching.BatchSize(3), batching.MaxThreads(1), batching.MinSizeForConcurrency(5))

		require.NoError(t, err)
		require.Equal(t, []int{11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21}, res)
	})

	t.Run("multi threaded batch by 1", func(t *testing.T) {
		ctx := context.Background()
		array := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}

		res, err := batching.Pipe(ctx, array, plus10,
			batching.BatchSize(1), batching.MaxThreads(100), batching.MinSizeForConcurrency(5))

		require.NoError(t, err)
		require.Equal(t, []int{11, 12, 13, 14, 15, 16, 17, 18, 19}, res)
	})

	t.Run("multi threaded batch by 3", func(t *testing.T) {
		ctx := context.Background()
		array := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}

		res, err := batching.Pipe(ctx, array, plus10,
			batching.BatchSize(3), batching.MaxThreads(100), batching.MinSizeForConcurrency(5))

		require.NoError(t, err)
		require.Equal(t, []int{11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21}, res)
	})

	t.Run("cancel context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		array := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
		var batches [][]int
		res, err := batching.Pipe(ctx, array, func(ctx context.Context, batch []int) ([]int, error) {
			// NOTE: second iteration may acquire semaphore and proceed with cancelled context.
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			default:
			}

			batches = append(batches, batch)
			cancel()
			return batch, nil
		}, batching.BatchSize(4), batching.MaxThreads(1), batching.MinSizeForConcurrency(5))

		require.Error(t, err)
		require.Nil(t, res)
		require.Equal(t, [][]int{{1, 2, 3, 4}}, batches)
	})

	t.Run("return error", func(t *testing.T) {
		ctx := context.Background()
		array := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
		var batches [][]int
		res, err := batching.Pipe(ctx, array, func(ctx context.Context, batch []int) ([]int, error) {
			batches = append(batches, batch)
			return nil, errors.New("intended")
		}, batching.BatchSize(4), batching.MaxThreads(1), batching.MinSizeForConcurrency(5))

		require.Error(t, err)
		require.Nil(t, res)
		require.Equal(t, [][]int{{1, 2, 3, 4}}, batches)
	})

	t.Run("return error in last batch", func(t *testing.T) {
		ctx := context.Background()
		array := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
		var batches [][]int
		res, err := batching.Pipe(ctx, array, func(ctx context.Context, batch []int) ([]int, error) {
			batches = append(batches, batch)
			return nil, errors.New("intended")
		}, batching.BatchSize(10), batching.MaxThreads(1), batching.MinSizeForConcurrency(5))

		require.Error(t, err)
		require.Nil(t, res)
		require.Equal(t, [][]int{{1, 2, 3, 4, 5, 6, 7, 8, 9}}, batches)
	})

	t.Run("panic", func(t *testing.T) {
		ctx := context.Background()
		array := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
		var batches [][]int

		require.PanicsWithValue(t, "intended", func() {
			_, _ = batching.Pipe(ctx, array, func(ctx context.Context, batch []int) ([]int, error) {
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
			_, _ = batching.Pipe(ctx, array, func(ctx context.Context, batch []int) ([]int, error) {
				batches = append(batches, batch)
				panic("intended")
			}, batching.BatchSize(10), batching.MaxThreads(1), batching.MinSizeForConcurrency(5))
		})

		require.Equal(t, [][]int{{1, 2, 3, 4, 5, 6, 7, 8, 9}}, batches)
	})
}
