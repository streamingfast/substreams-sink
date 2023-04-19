package sink

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"google.golang.org/grpc"

	"github.com/cenkalti/backoff/v4"
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/logging"
	"github.com/streamingfast/shutter"
	"github.com/streamingfast/substreams/client"
	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
	"go.uber.org/zap"
)

const defaultBlockDataBufferSize = 12

type Sinker struct {
	*shutter.Shutter

	clientConfig          *client.SubstreamsClientConfig
	modules               *pbsubstreams.Modules
	outputModule          *pbsubstreams.Module
	mode                  SubstreamsMode
	stats                 *Stats
	blockScopeDataHandler BlockScopeDataHandler

	// Options
	buffer        *blockDataBuffer
	infiniteRetry bool

	logger    *zap.Logger
	tracer    logging.Tracer
	forkSteps []pbsubstreams.ForkStep
}

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

func New(
	mode SubstreamsMode,
	modules *pbsubstreams.Modules,
	outputModule *pbsubstreams.Module,
	h BlockScopeDataHandler,
	clientConfig *client.SubstreamsClientConfig,
	forkSteps []pbsubstreams.ForkStep,
	logger *zap.Logger,
	tracer logging.Tracer,
	opts ...Option,
) (*Sinker, error) {
	s := &Sinker{
		Shutter:               shutter.New(),
		clientConfig:          clientConfig,
		modules:               modules,
		outputModule:          outputModule,
		mode:                  mode,
		blockScopeDataHandler: h,
		stats:                 newStats(logger),
		forkSteps:             forkSteps,
		logger:                logger,
		tracer:                tracer,
	}

	for _, opt := range opts {
		opt(s)
	}

	return s, nil
}

func (s *Sinker) Start(ctx context.Context, blockRange *bstream.Range, cursor *Cursor) error {
	s.OnTerminating(func(_ error) { s.stats.Close() })
	s.stats.OnTerminated(func(err error) { s.Shutdown(err) })
	s.stats.Start(2 * time.Second)

	var err error
	defer func() {
		s.Shutdown(err)
	}()

	err = s.run(ctx, blockRange, cursor)
	return err
}

func (s *Sinker) run(ctx context.Context, blockRange *bstream.Range, cursor *Cursor) (err error) {
	if s.buffer != nil && blockRange != nil {
		if blockRange.EndBlock() != nil {
			endBlockNum := new(uint64)
			*endBlockNum = *(blockRange.EndBlock()) + uint64(s.buffer.size)
			blockRange = bstream.NewRangeExcludingEnd(blockRange.StartBlock(), *endBlockNum)
		}
	}

	activeCursor := cursor
	if s.blockScopeDataHandler == nil {
		return fmt.Errorf("block scope data hanlder not set")
	}

	ssClient, closeFunc, callOpts, err := client.NewSubstreamsClient(s.clientConfig)
	if err != nil {
		return fmt.Errorf("new substreams client: %w", err)
	}
	s.OnTerminating(func(_ error) { closeFunc() })

	// We will wait at max approximatively 5m before dying
	var backOff backoff.BackOff = backoff.NewExponentialBackOff()
	if !s.infiniteRetry {
		backOff = backoff.WithMaxRetries(backOff, 15)
	}

	backOff = backoff.WithContext(backOff, ctx)

	startBlock := blockRange.StartBlock()
	stopBlock := uint64(0)
	if blockRange.EndBlock() != nil {
		stopBlock = *blockRange.EndBlock()
	}

	for {
		req := &pbsubstreams.Request{
			StartBlockNum:  int64(startBlock),
			StopBlockNum:   stopBlock,
			StartCursor:    activeCursor.Cursor,
			ForkSteps:      s.forkSteps,
			Modules:        s.modules,
			OutputModules:  []string{s.outputModule.Name},
			ProductionMode: s.mode == SubstreamsModeProduction,
		}

		var receivedMessage bool
		activeCursor, receivedMessage, err = s.doRequest(ctx, activeCursor, req, ssClient, callOpts)

		// If we received at least one message, we must reset the backoff
		if receivedMessage {
			backOff.Reset()
		}

		if err != nil {
			if errors.Is(err, io.EOF) {
				// We must assume that receiving an `io.EOF` means the stop block was reached. This is because
				// on network that can skips block number, it's possible that we requested to stop on a block
				// number that is no in the chain meaning we will receive `io.EOF` but the last seen block before
				// it is not our block number, we must have confidence in the Substreams provider to respect the
				// protocol
				s.logger.Info("substreams ended correctly, reached your stop block",
					zap.String("last_cursor", activeCursor.Cursor),
				)

				return nil
			}
			SubstreamsErrorCount.Inc()
			s.logger.Error("substreams encountered an error", zap.Error(err))

			sleepFor := backOff.NextBackOff()
			if sleepFor == backoff.Stop {
				s.logger.Info("backoff requested to stop retries")
				return err
			}

			s.logger.Info("sleeping before re-connecting", zap.Duration("sleep", sleepFor))
			time.Sleep(sleepFor)
		}
	}
}

func (s *Sinker) doRequest(
	ctx context.Context,
	activeCursor *Cursor,
	req *pbsubstreams.Request,
	ssClient pbsubstreams.StreamClient,
	callOpts []grpc.CallOption,
) (
	*Cursor,
	bool,
	error,
) {
	s.logger.Debug("launching substreams request", zap.Int64("start_block", req.StartBlockNum), zap.Stringer("cursor", activeCursor))
	receivedMessage := false

	progressMessageCount := 0
	stream, err := ssClient.Blocks(ctx, req, callOpts...)
	if err != nil {
		return activeCursor, receivedMessage, fmt.Errorf("call sf.substreams.v1.Stream/Blocks: %w", err)
	}

	for {
		if s.tracer.Enabled() {
			s.logger.Debug("substreams waiting to receive message", zap.String("cursor", activeCursor.Cursor))
		}

		resp, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return activeCursor, receivedMessage, err
			}

			return activeCursor, receivedMessage, fmt.Errorf("receive stream next message: %w", err)
		}

		receivedMessage = true

		switch r := resp.Message.(type) {
		case *pbsubstreams.Response_Progress:
			for _, module := range r.Progress.Modules {
				progressMessageCount++
				ProgressMessageCount.Inc(module.Name)
			}

			if s.tracer.Enabled() {
				s.logger.Debug("received response progress", zap.Reflect("progress", r))
			}

		case *pbsubstreams.Response_Data:
			var dataToProcess []*pbsubstreams.BlockScopedData

			if s.buffer == nil {
				dataToProcess = []*pbsubstreams.BlockScopedData{r.Data} // no buffering, process directly
			} else {
				block := bstream.NewBlockRef(r.Data.Clock.Id, r.Data.Clock.Number)
				cursor := NewCursor(r.Data.Cursor, block)
				activeCursor = cursor

				err = s.buffer.AddBlockData(r.Data)
				if err != nil {
					return activeCursor, receivedMessage, fmt.Errorf("buffer add block data: %w", err)
				}

				dataToProcess, err = s.buffer.GetBlockData()
				if err != nil {
					return activeCursor, receivedMessage, fmt.Errorf("get block data from buffer: %w", err)
				}
			}

			for _, blockData := range dataToProcess {
				block := bstream.NewBlockRef(blockData.Clock.Id, blockData.Clock.Number)
				currentCursor := NewCursor(blockData.Cursor, block)
				if err := s.blockScopeDataHandler(ctx, currentCursor, blockData); err != nil {
					return activeCursor, receivedMessage, fmt.Errorf("handle block scope data: %w", err)
				}

				HeadBlockNumber.SetUint64(block.Num())
				s.stats.RecordBlock(block)
				BlockCount.AddInt(1)
			}
		case *pbsubstreams.Response_Session:
			s.logger.Info("session init", zap.String("trace_id", r.Session.TraceId))
		default:
			s.logger.Info("received unknown type of message", zap.Reflect("message", r))
		}
	}
}

func stepsContainUndo(forkSteps []pbsubstreams.ForkStep) bool {
	for _, step := range forkSteps {
		if step == pbsubstreams.ForkStep_STEP_UNDO {
			return true
		}
	}
	return false
}
