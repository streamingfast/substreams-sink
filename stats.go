package sink

import (
	"time"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/dmetrics"
	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
)

type Stats struct {
	*shutter.Shutter

	progressMsgRate *dmetrics.AvgRatePromCounter
	dataMsgRate     *dmetrics.AvgRatePromCounter
	undoMsgRate     *dmetrics.AvgRatePromCounter

	lastBlock bstream.BlockRef
	logger    *zap.Logger
}

func newStats(logger *zap.Logger) *Stats {
	return &Stats{
		Shutter: shutter.New(),

		progressMsgRate: dmetrics.MustNewAvgRateFromPromCounter(ProgressMessageCount, 1*time.Second, 30*time.Second, "msg"),
		dataMsgRate:     dmetrics.MustNewAvgRateFromPromCounter(DataMessageCount, 1*time.Second, 30*time.Second, "msg"),
		undoMsgRate:     dmetrics.MustNewAvgRateFromPromCounter(UndoMessageCount, 1*time.Second, 30*time.Second, "msg"),
		lastBlock:       unsetBlockRef{},

		logger: logger,
	}
}

func (s *Stats) RecordBlock(block bstream.BlockRef) {
	s.lastBlock = block
}

func (s *Stats) Start(each time.Duration) {
	if s.IsTerminating() || s.IsTerminated() {
		panic("already shutdown, refusing to start again")
	}

	go func() {
		ticker := time.NewTicker(each)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				s.LogNow()
			case <-s.Terminating():
				return
			}
		}
	}()
}

func (s *Stats) LogNow() {
	// Logging fields order is important as it affects the final rendering, we carefully ordered
	// them so the development logs looks nicer.
	s.logger.Info("substreams stream stats",
		zap.Stringer("data_msg_rate", s.dataMsgRate),
		zap.Stringer("progress_msg_rate", s.progressMsgRate),
		zap.Stringer("undo_msg_rate", s.undoMsgRate),
		zap.Stringer("last_block", s.lastBlock),
	)
}

func (s *Stats) Close() {
	s.LogNow()
	s.Shutdown(nil)
}

type unsetBlockRef struct{}

func (unsetBlockRef) ID() string     { return "" }
func (unsetBlockRef) Num() uint64    { return 0 }
func (unsetBlockRef) String() string { return "None" }
