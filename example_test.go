package batching_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/golang-one/batching"
)

func TestExample(t *testing.T) {
	t.SkipNow()

	t.Run("Do", func(t *testing.T) {
		var (
			ctx   = context.Background()
			start = time.Now()
			tasks = []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
		)

		err := batching.Do(ctx, tasks, func(ctx context.Context, batch []int) error {
			fmt.Println("processing", batch)
			time.Sleep(time.Second)
			return nil
		}, batching.BatchSize(2), batching.MaxThreads(4), batching.MinSizeForConcurrency(0))

		require.NoError(t, err)

		fmt.Println(time.Since(start))
	})

	t.Run("Pipe", func(t *testing.T) {
		var (
			ctx   = context.Background()
			start = time.Now()
			tasks = []int{1, 2, 3, 4, 5, 6, 7, 8, 9}
		)

		res, err := batching.Pipe(ctx, tasks, func(ctx context.Context, batch []int) ([]int, error) {
			fmt.Println("processing", batch)
			time.Sleep(time.Second)

			ret := make([]int, len(batch))
			for i, v := range batch {
				ret[i] = v * v
			}
			return ret, nil
		}, batching.BatchSize(2), batching.MaxThreads(4), batching.MinSizeForConcurrency(0))

		require.NoError(t, err)
		require.Equal(t, []int{1, 4, 9, 16, 25, 36, 49, 64, 81}, res)

		fmt.Println(time.Since(start))
	})
}
