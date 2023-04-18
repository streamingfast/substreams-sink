package sink

import (
	"time"

	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
)

type LivenessCheck interface {
	IsLive(block *pbsubstreams.Clock) bool
}

type LivenessChecker struct {
	delta   time.Duration
	nowFunc func() time.Time

	isLive bool
}

func NewLivenessChecker(delta time.Duration) *LivenessChecker {
	return &LivenessChecker{
		delta:   delta,
		nowFunc: time.Now,
	}
}

func (t *LivenessChecker) IsLive(clock *pbsubstreams.Clock) bool {
	if t.isLive {
		return true
	}

	if clock == nil {
		return false
	}

	blockTimeStamp := clock.GetTimestamp()
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
