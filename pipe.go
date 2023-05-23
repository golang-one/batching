package batching

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
)

// Pipe divides tasks into batches and runs provided function for each of them.
// Pipe returns nil and the first error encountered, or processed results and nil if there were no errors.
// Pipe panics on calling goroutine if provided funcion panics.
func Pipe[F, R any](
	ctx context.Context,
	tasks []F,
	pipe func(context.Context, []F) ([]R, error),
	options ...DoOption,
) ([]R, error) {
	doOpts := doOptions{
		batchSize:             1,
		minSizeForConcurrency: 2,
		maxThreads:            runtime.NumCPU(),
	}

	for _, do := range options {
		doOpts = do.do(doOpts)
	}

	if len(tasks) < doOpts.minSizeForConcurrency {
		return pipe(ctx, tasks)
	}

	var (
		doCtx, cancel = context.WithCancel(ctx)
		semaphore     = make(chan struct{}, doOpts.maxThreads)

		firstError atomic.Pointer[error]
		firstPanic atomic.Pointer[any]

		wg sync.WaitGroup

		res   = make([]R, len(tasks))
		index = 0
	)

	defer cancel()
	defer close(semaphore)

	for len(tasks) > 0 {
		thisBatchSize := doOpts.batchSize
		if len(tasks) < thisBatchSize {
			thisBatchSize = len(tasks)
		}
		batch := tasks[:thisBatchSize]
		tasks = tasks[thisBatchSize:]

		batchStart := index
		index += thisBatchSize

		select {
		case <-doCtx.Done():
			if fp := firstPanic.Load(); fp != nil {
				panic(*fp)
			}

			ctxErr := doCtx.Err()
			firstError.CompareAndSwap(nil, &ctxErr)
			return nil, *firstError.Load()
		case semaphore <- struct{}{}:
		}

		wg.Add(1)

		go func() {
			defer func() {
				if rec := recover(); rec != nil {
					firstPanic.CompareAndSwap(nil, &rec)
					cancel()
				}

				select {
				case <-doCtx.Done():
					ctxErr := doCtx.Err()
					firstError.CompareAndSwap(nil, &ctxErr)
				case <-semaphore:
				}

				wg.Done()
			}()

			batchRes, pipeErr := pipe(doCtx, batch)
			if pipeErr != nil {
				firstError.CompareAndSwap(nil, &pipeErr)
				cancel()
				return
			}

			for i := 0; i < thisBatchSize; i++ {
				res[i+batchStart] = batchRes[i]
			}
		}()
	}

	wg.Wait()

	if fp := firstPanic.Load(); fp != nil {
		panic(*fp)
	}

	if err := firstError.Load(); err != nil {
		return nil, *err
	}

	return res, nil
}
