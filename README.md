# Batching

[![MIT License](http://img.shields.io/badge/license-MIT-blue.svg?style=flat)](LICENSE)
[![GoDoc](https://godoc.org/github.com/golang-one/batching?status.svg)](https://godoc.org/github.com/golang-one/batching)
[![CI](https://github.com/golang-one/batching/actions/workflows/ci.yaml/badge.svg)](https://github.com/golang-one/batching/actions/workflows/ci.yaml)
[![Go Report Card](https://goreportcard.com/badge/github.com/golang-one/batching)](https://goreportcard.com/report/github.com/golang-one/batching)
[![Coverage](https://coveralls.io/repos/github/golang-one/batching/badge.svg?branch=master)](https://coveralls.io/github/golang-one/batching?branch=master)

## Motivation

This package is designed as a helper to improve the efficiency and performance of applications that deal with large amounts of data or tasks. By splitting a set of tasks into smaller batches and processing them in parallel, the overall latency is reduced, resulting in faster and more efficient execution.

## Use

Install the package:
```
$ go get github.com/golang-one/batching
```

Use `batching.Do` for operations which do not return results:
```go
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

/*
    processing [7 8]
    processing [3 4]
    processing [1 2]
    processing [5 6]
    processing [9]
    2.001441515s
*/
```

Use `batching.Pipe` for operations which return results:
```go
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

/*
    processing [7 8]
    processing [5 6]
    processing [3 4]
    processing [1 2]
    processing [9]
    2.001943909s
*/
```
