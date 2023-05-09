package sink

import (
	"testing"

	"github.com/spf13/pflag"
	"github.com/stretchr/testify/assert"
)

func TestAddFlagsToSet(t *testing.T) {
	tests := []struct {
		name          string
		ignore        []FlagIgnored
		expectedFlags []string
	}{
		{
			"default",
			nil,
			[]string{
				FlagInsecure,
				FlagPlaintext,
				FlagUndoBufferSize,
				FlagLiveBlockTimeDelta,
				FlagDevelopmentMode,
				FlagFinalBlocksOnly,
				FlagInfiniteRetry,
				FlagIrreversibleOnly,
			},
		},
		{
			"ignore one",
			[]FlagIgnored{FlagIgnore(FlagInsecure)},
			[]string{
				FlagPlaintext,
				FlagUndoBufferSize,
				FlagLiveBlockTimeDelta,
				FlagDevelopmentMode,
				FlagFinalBlocksOnly,
				FlagInfiniteRetry,
				FlagIrreversibleOnly,
			},
		},
		{
			"ignore one multiple",
			[]FlagIgnored{FlagIgnore(FlagInsecure, FlagPlaintext)},
			[]string{
				FlagUndoBufferSize,
				FlagLiveBlockTimeDelta,
				FlagDevelopmentMode,
				FlagFinalBlocksOnly,
				FlagInfiniteRetry,
				FlagIrreversibleOnly,
			},
		},
		{
			"ignore multiple",
			[]FlagIgnored{FlagIgnore(FlagInsecure), FlagIgnore(FlagPlaintext)},
			[]string{
				FlagUndoBufferSize,
				FlagLiveBlockTimeDelta,
				FlagDevelopmentMode,
				FlagFinalBlocksOnly,
				FlagInfiniteRetry,
				FlagIrreversibleOnly,
			},
		},
		{
			"ignore mixed",
			[]FlagIgnored{FlagIgnore(FlagInsecure), FlagIgnore(FlagPlaintext, FlagLiveBlockTimeDelta)},
			[]string{
				FlagUndoBufferSize,
				FlagDevelopmentMode,
				FlagFinalBlocksOnly,
				FlagInfiniteRetry,
				FlagIrreversibleOnly,
			},
		},
		{
			"ignore final block",
			[]FlagIgnored{FlagIgnore(FlagFinalBlocksOnly)},
			[]string{
				FlagInsecure,
				FlagPlaintext,
				FlagUndoBufferSize,
				FlagLiveBlockTimeDelta,
				FlagDevelopmentMode,
				FlagInfiniteRetry,
			},
		},
		{
			"ignore irreversible only",
			[]FlagIgnored{FlagIgnore(FlagIrreversibleOnly)},
			[]string{
				FlagInsecure,
				FlagPlaintext,
				FlagUndoBufferSize,
				FlagLiveBlockTimeDelta,
				FlagDevelopmentMode,
				FlagFinalBlocksOnly,
				FlagInfiniteRetry,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			flagSet := pflag.NewFlagSet("", pflag.ContinueOnError)

			AddFlagsToSet(flagSet, tt.ignore...)

			var actualFlags []string
			flagSet.VisitAll(func(f *pflag.Flag) {
				actualFlags = append(actualFlags, f.Name)
			})

			assert.ElementsMatch(t, tt.expectedFlags, actualFlags)
		})
	}
}
