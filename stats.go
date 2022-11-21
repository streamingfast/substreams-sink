package sink

import (
	"time"

	"github.com/streamingfast/dmetrics"

	"github.com/streamingfast/bstream"
	"github.com/streamingfast/shutter"
	"go.uber.org/zap"
)

type Stats struct {
	*shutter.Shutter

	progressMsgRate *dmetrics.AvgRatePromCounter
	blockRate       *dmetrics.AvgRatePromCounter

	lastBlock bstream.BlockRef
	logger    *zap.Logger
}

func newStats(logger *zap.Logger) *Stats {
	return &Stats{
		Shutter: shutter.New(),

		progressMsgRate: dmetrics.MustNewAvgRateFromPromCounter(ProgressMessageCount, 1*time.Second, 30*time.Second, "msg"),
		blockRate:       dmetrics.MustNewAvgRateFromPromCounter(BlockCount, 1*time.Second, 30*time.Second, "blocks"),

		logger: logger,
	}
}

func (s *Stats) RecordBlock(block bstream.BlockRef) {
	s.lastBlock = block
}

func (s *Stats) Start(each time.Duration) {
	s.logger.Info("starting stats service", zap.Duration("runs_each", each))

	if s.IsTerminating() || s.IsTerminated() {
		panic("already shutdown, refusing to start again")
	}

	go func() {
		ticker := time.NewTicker(each)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				// Logging fields order is important as it affects the final rendering, we carefully ordered
				// them so the development logs looks nicer.
				fields := []zap.Field{
					zap.Stringer("progress_msg_rate", s.progressMsgRate),
					zap.Stringer("block_rate", s.blockRate),
				}

				if s.lastBlock == nil {
					fields = append(fields, zap.String("last_block", "None"))
				} else {
					fields = append(fields, zap.Stringer("last_block", s.lastBlock))
				}

				s.logger.Info("substreams lidar stats", fields...)
			case <-s.Terminating():
				break
			}
		}
	}()
}

func (s *Stats) Close() {
	s.Shutdown(nil)
}
