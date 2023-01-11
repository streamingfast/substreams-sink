package sink

import (
	"sync"
	"time"

	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
)

type LivenessChecker struct {
	delta   time.Duration
	nowFunc func() time.Time

	isLive bool

	mu sync.RWMutex
}

func NewLivenessChecker(delta time.Duration) *LivenessChecker {
	return &LivenessChecker{
		delta:   delta,
		nowFunc: time.Now,
	}
}

func (t *LivenessChecker) IsLive(block *pbsubstreams.BlockScopedData) bool {
	if t.isLive {
		return true
	}

	if block == nil || block.Clock == nil {
		return false
	}

	blockTimeStamp := block.Clock.GetTimestamp()
	if blockTimeStamp == nil {
		return false
	}

	blockTime := blockTimeStamp.AsTime()
	nowTime := t.nowFunc()

	if nowTime.Sub(blockTime) <= t.delta {
		t.isLive = true
	}

	return t.isLive
}
