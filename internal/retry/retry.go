package retry

import (
	"time"

	retrygo "github.com/avast/retry-go/v4"
)

var DefaultOptions = []retrygo.Option{
	retrygo.LastErrorOnly(true),
	retrygo.Delay(time.Second),
	retrygo.DelayType(retrygo.FixedDelay),
}

type Config[T any] struct {
	If      func(err error) bool
	Options []retrygo.Option
}

func (rc Config[T]) Do(f retrygo.RetryableFuncWithData[T]) (T, error) {
	return retrygo.DoWithData(f, rc.Options...)
}

func OnErrorConfig[T any](attemptCount uint, check func(error) bool) Config[T] {
	cfg := Config[T]{
		If:      check,
		Options: []retrygo.Option{retrygo.Attempts(attemptCount)},
	}
	cfg.Options = append(cfg.Options, DefaultOptions...)
	return cfg
}
