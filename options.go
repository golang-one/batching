package batching

type (
	// DoOption is an optional parameter for Do and Pipe functions.
	DoOption interface {
		do(options doOptions) doOptions
	}

	// BatchSize is a size of a batch into which the tasks are divided.
	BatchSize int

	// MinSizeForConcurrency is the minimum number of tasks to start additional goroutines.
	// For fewer tasks, a calling goroutine is used.
	MinSizeForConcurrency int

	// MaxThreads is the maximum number of simultaneously running goroutines.
	MaxThreads int
)

type doOptions struct {
	batchSize             int
	minSizeForConcurrency int
	maxThreads            int
}

func (o BatchSize) do(options doOptions) doOptions {
	if o >= 1 {
		options.batchSize = int(o)
	}

	return options
}

func (o MinSizeForConcurrency) do(options doOptions) doOptions {
	if o > 1 {
		options.minSizeForConcurrency = int(o)
	}

	return options
}

func (o MaxThreads) do(options doOptions) doOptions {
	if o > 0 {
		options.maxThreads = int(o)
	}

	return options
}
