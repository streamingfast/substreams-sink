package sink

import (
	"testing"
	"time"

	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestLivenessChecker_IsLive(t *testing.T) {
	nowCalls := 0
	tnow, _ := time.Parse(time.RFC3339, "2023-01-01T00:00:00Z")
	nowFunc := func() time.Time {
		nowCalls++
		return tnow
	}

	tests := []struct {
		block              *pbsubstreamsrpc.BlockScopedData
		expectedResult     bool
		expectedTimeChecks int
	}{
		{testBlock("1a", 1, tnow.Add(-5*time.Second)), false, 1},
		{testBlock("2a", 2, tnow.Add(-4*time.Second)), false, 2},
		{testBlock("3a", 3, tnow.Add(-3*time.Second)), true, 3}, //threshold reached
		{testBlock("4a", 4, tnow.Add(-2*time.Second)), true, 3},
		{testBlock("5a", 5, tnow.Add(-1*time.Second)), true, 3},
	}

	livenessChecker := NewLivenessChecker(3 * time.Second)
	livenessChecker.nowFunc = nowFunc

	for _, tt := range tests {
		res := livenessChecker.IsLive(tt.block)
		if res != tt.expectedResult {
			t.Errorf("expected result %v, got %v", tt.expectedResult, res)
		}
		if nowCalls != tt.expectedTimeChecks {
			t.Errorf("expected %d time checks, got %d", tt.expectedTimeChecks, nowCalls)
		}
	}

}

func testBlock(id string, num uint64, time time.Time) *pbsubstreamsrpc.BlockScopedData {
	blockData := &pbsubstreamsrpc.BlockScopedData{
		Cursor: "",
		Clock: &pbsubstreams.Clock{
			Id:        id,
			Number:    num,
			Timestamp: timestamppb.New(time),
		},
	}

	return blockData
}
