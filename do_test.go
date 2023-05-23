package batching

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestDo(t *testing.T) {
	defer goleak.VerifyNone(t)

	t.Run("min size for concurrency not reached", func(t *testing.T) {
		ctx := context.Background()
		array := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
		var batches [][]int
		err := Do(ctx, array, func(ctx context.Context, batch []int) error {
			batches = append(batches, batch)
			return nil
		}, BatchSize(1), MaxThreads(1), MinSizeForConcurrency(100))

		require.NoError(t, err)
		require.Equal(t, [][]int{{1, 2, 3, 4, 5, 6, 7, 8, 9}}, batches)
	})

	t.Run("single threaded batch by 1", func(t *testing.T) {
		ctx := context.Background()
		array := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
		var batches [][]int
		err := Do(ctx, array, func(ctx context.Context, batch []int) error {
			batches = append(batches, batch)
			return nil
		}, BatchSize(1), MaxThreads(1), MinSizeForConcurrency(5))

		require.NoError(t, err)
		require.Equal(t, [][]int{{1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}}, batches)
	})

	t.Run("single threaded batch by 3", func(t *testing.T) {
		ctx := context.Background()
		array := []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}
		var batches [][]int
		err := Do(ctx, array, func(ctx context.Context, batch []int) error {
			batches = append(batches, batch)
			return nil
		}, BatchSize(3), MaxThreads(1), MinSizeForConcurrency(5))

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
		err := Do(ctx, array, func(ctx context.Context, batch []int) error {
			mx.Lock()
			defer mx.Unlock()
			batches = append(batches, batch)
			return nil
		}, BatchSize(1), MaxThreads(100), MinSizeForConcurrency(5))

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
		err := Do(ctx, array, func(ctx context.Context, batch []int) error {
			mx.Lock()
			defer mx.Unlock()
			batches = append(batches, batch)
			return nil
		}, BatchSize(3), MaxThreads(100), MinSizeForConcurrency(5))

		require.NoError(t, err)
		require.ElementsMatch(t, [][]int{{1, 2, 3}, {4, 5, 6}, {7, 8, 9}, {10, 11}}, batches)
	})

	t.Run("cancel context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		array := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
		var batches [][]int
		err := Do(ctx, array, func(ctx context.Context, batch []int) error {
			batches = append(batches, batch)
			cancel()
			return nil
		}, BatchSize(4), MaxThreads(1), MinSizeForConcurrency(5))

		require.Error(t, err)
		require.Equal(t, [][]int{{1, 2, 3, 4}}, batches)
	})

	t.Run("return error", func(t *testing.T) {
		ctx := context.Background()
		array := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
		var batches [][]int
		err := Do(ctx, array, func(ctx context.Context, batch []int) error {
			batches = append(batches, batch)
			return errors.New("intended")
		}, BatchSize(4), MaxThreads(1), MinSizeForConcurrency(5))

		require.Error(t, err)
		require.Equal(t, [][]int{{1, 2, 3, 4}}, batches)
	})

	t.Run("return error in last batch", func(t *testing.T) {
		ctx := context.Background()
		array := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
		var batches [][]int
		err := Do(ctx, array, func(ctx context.Context, batch []int) error {
			batches = append(batches, batch)
			return errors.New("intended")
		}, BatchSize(10), MaxThreads(1), MinSizeForConcurrency(5))

		require.Error(t, err)
		require.Equal(t, [][]int{{1, 2, 3, 4, 5, 6, 7, 8, 9}}, batches)
	})

	t.Run("panic", func(t *testing.T) {
		ctx := context.Background()
		array := []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
		var batches [][]int

		require.PanicsWithValue(t, "intended", func() {
			_ = Do(ctx, array, func(ctx context.Context, batch []int) error {
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
			_ = Do(ctx, array, func(ctx context.Context, batch []int) error {
				batches = append(batches, batch)
				panic("intended")
			}, BatchSize(10), MaxThreads(1), MinSizeForConcurrency(5))
		})

		require.Equal(t, [][]int{{1, 2, 3, 4, 5, 6, 7, 8, 9}}, batches)
	})
}
