package sink

import (
	"fmt"
	"sort"
	"sync"

	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
)

type blockDataBuffer struct {
	size              int
	irrIdx            int
	lastBlockReturned uint64

	data []*pbsubstreams.BlockScopedData

	mu sync.RWMutex
}

func newBlockDataBuffer(size int) *blockDataBuffer {
	return &blockDataBuffer{
		size: size,
		data: make([]*pbsubstreams.BlockScopedData, 0, size),
	}
}

func (b *blockDataBuffer) AddBlockData(blockData *pbsubstreams.BlockScopedData) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch blockData.Step {
	case pbsubstreams.ForkStep_STEP_NEW:
		return b.handleNew(blockData)
	case pbsubstreams.ForkStep_STEP_IRREVERSIBLE:
		return b.handleIrreversible(blockData)
	case pbsubstreams.ForkStep_STEP_UNDO:
		return b.handleUndo(blockData)
	}

	return nil
}

func (b *blockDataBuffer) GetBlockData() ([]*pbsubstreams.BlockScopedData, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.data) >= b.size {
		ix := len(b.data) - b.size
		if b.irrIdx > ix {
			ix = b.irrIdx
		}

		blocks := b.data[:ix]
		b.data = b.data[ix:]
		b.irrIdx = 0
		b.lastBlockReturned = blocks[len(blocks)-1].Clock.Number

		return blocks, nil
	} else if b.irrIdx != 0 {
		blocks := b.data[0:b.irrIdx]
		b.data = b.data[b.irrIdx:]
		b.irrIdx = 0

		b.lastBlockReturned = blocks[len(blocks)-1].Clock.Number
		return blocks, nil
	}

	return nil, nil
}

func (b *blockDataBuffer) handleUndo(blockData *pbsubstreams.BlockScopedData) error {
	if len(b.data) == 0 {
		return nil
	}

	if b.lastBlockReturned >= blockData.Clock.Number {
		return fmt.Errorf("cannot undo block %d, last block returned is %d", blockData.Clock.Number, b.lastBlockReturned)
	}

	for i := len(b.data) - 1; i >= 0; i-- {
		if b.data[i].Clock.Number >= blockData.Clock.Number {
			b.data = b.data[0:i]
		} else {
			break
		}
	}

	return nil
}

func (b *blockDataBuffer) handleNew(blockData *pbsubstreams.BlockScopedData) error {
	b.data = append(b.data, blockData)

	sort.Slice(b.data, func(i, j int) bool {
		return b.data[i].Clock.Number < b.data[j].Clock.Number
	})

	return nil
}

func (b *blockDataBuffer) handleIrreversible(blockData *pbsubstreams.BlockScopedData) error {
	b.data = append(b.data, blockData)
	b.irrIdx = len(b.data)

	sort.Slice(b.data, func(i, j int) bool {
		return b.data[i].Clock.Number < b.data[j].Clock.Number
	})

	return nil
}
