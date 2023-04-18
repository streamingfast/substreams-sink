package sink

import "github.com/streamingfast/bstream"

type Option func(s *Sinker)

// WithBlockDataBuffer creates a buffer of block data which is used to handle undo fork steps.
//
// Ensure that this buffer is large enough to capture all block reorganizations.
// If the buffer is too small, the sinker will not be able to handle the reorganization and will error if an undo is received for a block which has already been returned to the sink.
// If the buffer is too large, the sinker will take more time than necessary to write data to the sink.
//
// If the sink is configured to handle irreversible blocks, the default buffer size is 12.
// If the sink is not configured to handle undo fork steps, the buffer is not used.
func WithBlockDataBuffer(bufferSize int) Option {
	return func(s *Sinker) {
		buffer := newBlockDataBuffer(bufferSize)
		s.buffer = buffer
	}
}

// WithInfiniteRetry remove the maximum retry limit of 15 (hard-coded right now)
// which spans approximatively 5m so that retry is perform indefinitely without
// never exiting the process.
func WithInfiniteRetry() Option {
	return func(s *Sinker) {
		s.infiniteRetry = true
	}
}

// WithFinalBlockOnly configures a the [Sinker] to which itself configurs the Substreams
// gRPC stream to only send [pbsubstreamsrpc.BlockScopedData] once the block is final, this
// means that `WithBlockDataBuffer` if used has is discarded and [BlockUndoSignalHandler]
// will never be called.
func WithFinalBlockOnly() Option {
	return func(s *Sinker) {
		s.finalBlocksOnly = true
	}
}

// WithLivenessChecker configures a [LivnessCheck] on the [Sinker] instance.
//
// By configuring a liveness checker, the [MessageContext] received by [BlockScopedDataHandler]
// and [BlockUndoSignalHandler] will have the field [MessageContext.IsLive] properly populated.
func WithLivenessChecker(livenessChecker LivenessCheck) Option {
	return func(s *Sinker) {
		s.livenessChecker = livenessChecker
	}
}

// WithBlockRange configures the [Sinker] instance to only stream for the range specified. If
// there is no range specified on the [Sinker], the [Sinker] is going to sink automatically
// from module's start block to live never ending.
func WithBlockRange(blockRange *bstream.Range) Option {
	return func(s *Sinker) {
		s.blockRange = blockRange
	}
}
