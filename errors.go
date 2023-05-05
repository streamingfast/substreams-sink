package sink

import (
	"errors"
	"fmt"
)

var ErrBackOffExpired = errors.New("unable to complete work within backoff time limit")

// RetryableError can be returned by your handler either [SinkerHandlers#HandleBlockScopedData] or
// [SinkerHandlers#HandleBlockUndoSignal] to notify the sinker that it's a retryable error and the
// stream can continue
type RetryableError struct {
	original error
}

// NewRetryableError creates a new [RetryableError] struct ensuring `original` error is non-nil
// otherwise this function panics with an error.
func NewRetryableError(original error) *RetryableError {
	if original == nil {
		panic(fmt.Errorf("the 'original' argument is mandatory"))
	}

	return &RetryableError{original}
}

func (r *RetryableError) Unwrap() error {
	return r.original
}

func (r *RetryableError) Error() string {
	return fmt.Sprintf("%s (retryable)", r.original)
}
