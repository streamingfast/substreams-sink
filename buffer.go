package sink

import (
	"sort"
	"sync"

	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
)

type BlockDataBuffer struct {
	size   int
	irrIdx int
	data   []*pbsubstreams.BlockScopedData

	mu sync.RWMutex
}

func NewBlockDataBuffer(size int) *BlockDataBuffer {
	return &BlockDataBuffer{
		size: size,
		data: make([]*pbsubstreams.BlockScopedData, 0, size),
	}
}

func (b *BlockDataBuffer) AddBlockData(blockData *pbsubstreams.BlockScopedData) error {
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

func (b *BlockDataBuffer) GetBlockData() ([]*pbsubstreams.BlockScopedData, error) {
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

		return blocks, nil
	} else if b.irrIdx != 0 {
		blocks := b.data[0:b.irrIdx]
		b.data = b.data[b.irrIdx:]
		b.irrIdx = 0

		return blocks, nil
	}

	return nil, nil
}

func (b *BlockDataBuffer) handleUndo(blockData *pbsubstreams.BlockScopedData) error {
	if len(b.data) == 0 {
		return nil
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

func (b *BlockDataBuffer) handleNew(blockData *pbsubstreams.BlockScopedData) error {
	b.data = append(b.data, blockData)

	sort.Slice(b.data, func(i, j int) bool {
		return b.data[i].Clock.Number < b.data[j].Clock.Number
	})

	return nil
}

func (b *BlockDataBuffer) handleIrreversible(blockData *pbsubstreams.BlockScopedData) error {
	b.data = append(b.data, blockData)
	b.irrIdx = len(b.data)

	sort.Slice(b.data, func(i, j int) bool {
		return b.data[i].Clock.Number < b.data[j].Clock.Number
	})

	return nil
}
