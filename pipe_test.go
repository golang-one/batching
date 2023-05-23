package batching

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
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
		res, err := Pipe(ctx, array, plus10, BatchSize(1), MaxThreads(1), MinSizeForConcurrency(100))

		require.NoError(t, err)
		require.Equal(t, []int{11, 12, 13, 14, 15, 16, 17, 18, 19}, res)
	})

	t.Run("single threaded batch by 1", func(t *testing.T) {
		ctx := context.Background()
		array := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
		res, err := Pipe(ctx, array, plus10, BatchSize(1), MaxThreads(1), MinSizeForConcurrency(5))

		require.NoError(t, err)
		require.Equal(t, []int{11, 12, 13, 14, 15, 16, 17, 18, 19}, res)
	})

	t.Run("single threaded batch by 3", func(t *testing.T) {
		ctx := context.Background()
		array := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}
		res, err := Pipe(ctx, array, plus10, BatchSize(3), MaxThreads(1), MinSizeForConcurrency(5))

		require.NoError(t, err)
		require.Equal(t, []int{11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21}, res)
	})

	t.Run("multi threaded batch by 1", func(t *testing.T) {
		ctx := context.Background()
		array := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}

		res, err := Pipe(ctx, array, plus10, BatchSize(1), MaxThreads(100), MinSizeForConcurrency(5))

		require.NoError(t, err)
		require.Equal(t, []int{11, 12, 13, 14, 15, 16, 17, 18, 19}, res)
	})

	t.Run("multi threaded batch by 3", func(t *testing.T) {
		ctx := context.Background()
		array := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}

		res, err := Pipe(ctx, array, plus10, BatchSize(3), MaxThreads(100), MinSizeForConcurrency(5))

		require.NoError(t, err)
		require.Equal(t, []int{11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21}, res)
	})

	t.Run("cancel context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		array := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
		var batches [][]int
		res, err := Pipe(ctx, array, func(ctx context.Context, batch []int) ([]int, error) {
			batches = append(batches, batch)
			cancel()
			return batch, nil
		}, BatchSize(4), MaxThreads(1), MinSizeForConcurrency(5))

		require.Error(t, err)
		require.Nil(t, res)
		require.Equal(t, [][]int{{1, 2, 3, 4}}, batches)
	})

	t.Run("return error", func(t *testing.T) {
		ctx := context.Background()
		array := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
		var batches [][]int
		res, err := Pipe(ctx, array, func(ctx context.Context, batch []int) ([]int, error) {
			batches = append(batches, batch)
			return nil, errors.New("intended")
		}, BatchSize(4), MaxThreads(1), MinSizeForConcurrency(5))

		require.Error(t, err)
		require.Nil(t, res)
		require.Equal(t, [][]int{{1, 2, 3, 4}}, batches)
	})

	t.Run("return error in last batch", func(t *testing.T) {
		ctx := context.Background()
		array := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
		var batches [][]int
		res, err := Pipe(ctx, array, func(ctx context.Context, batch []int) ([]int, error) {
			batches = append(batches, batch)
			return nil, errors.New("intended")
		}, BatchSize(10), MaxThreads(1), MinSizeForConcurrency(5))

		require.Error(t, err)
		require.Nil(t, res)
		require.Equal(t, [][]int{{1, 2, 3, 4, 5, 6, 7, 8, 9}}, batches)
	})

	t.Run("panic", func(t *testing.T) {
		ctx := context.Background()
		array := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
		var batches [][]int

		require.PanicsWithValue(t, "intended", func() {
			_, _ = Pipe(ctx, array, func(ctx context.Context, batch []int) ([]int, error) {
				batches = append(batches, batch)
				panic("intended")
			}, BatchSize(4), MaxThreads(1), MinSizeForConcurrency(5))
		})

		require.Equal(t, [][]int{{1, 2, 3, 4}}, batches)
	})

	t.Run("panic in last batch", func(t *testing.T) {
		ctx := context.Background()
		array := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
		var batches [][]int

		require.PanicsWithValue(t, "intended", func() {
			_, _ = Pipe(ctx, array, func(ctx context.Context, batch []int) ([]int, error) {
				batches = append(batches, batch)
				panic("intended")
			}, BatchSize(10), MaxThreads(1), MinSizeForConcurrency(5))
		})

		require.Equal(t, [][]int{{1, 2, 3, 4, 5, 6, 7, 8, 9}}, batches)
	})
}
