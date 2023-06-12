package batching

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
)

// Do divides tasks into batches and runs provided function for each of them.
// Do returns the first error encountered, or nil if there were no errors.
// Do panics on calling goroutine if provided function panics.
func Do[T any](
	ctx context.Context,
	tasks []T,
	do func(context.Context, []T) error,
	options ...DoOption,
) error {
	doOpts := doOptions{
		batchSize:             1,
		minSizeForConcurrency: 2,
		maxThreads:            runtime.NumCPU(),
	}

	for _, do := range options {
		doOpts = do.do(doOpts)
	}

	if len(tasks) < doOpts.minSizeForConcurrency {
		return do(ctx, tasks)
	}

	var (
		doCtx, cancel = context.WithCancel(ctx)
		semaphore     = make(chan struct{}, doOpts.maxThreads)

		firstError atomic.Pointer[error]
		firstPanic atomic.Pointer[any]

		wg sync.WaitGroup
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

		select {
		case <-doCtx.Done():
			if fp := firstPanic.Load(); fp != nil {
				panic(*fp)
			}

			ctxErr := doCtx.Err()
			firstError.CompareAndSwap(nil, &ctxErr)
			return *firstError.Load()
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

			doErr := do(doCtx, batch)
			if doErr != nil {
				firstError.CompareAndSwap(nil, &doErr)
				cancel()
			}
		}()
	}

	wg.Wait()

	if fp := firstPanic.Load(); fp != nil {
		panic(*fp)
	}

	if err := firstError.Load(); err != nil {
		return *err
	}

	return nil
}
