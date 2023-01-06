package sink

import (
	"sort"
	"sync"

	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
)

type BlockBuffer struct {
	size   int
	blocks []*pbsubstreams.BlockScopedData

	mu sync.RWMutex
}

func NewBlockBuffer(size int) *BlockBuffer {
	return &BlockBuffer{
		size:   size,
		blocks: make([]*pbsubstreams.BlockScopedData, 0, size),
	}
}

func (b *BlockBuffer) AddBlock(block *pbsubstreams.BlockScopedData) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	switch block.Step {
	case pbsubstreams.ForkStep_STEP_NEW:
		return b.handleNew(block)
	case pbsubstreams.ForkStep_STEP_UNDO:
		return b.handleUndo(block)
	}

	return nil
}

func (b *BlockBuffer) GetBlocks() ([]*pbsubstreams.BlockScopedData, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.blocks) < b.size {
		return nil, nil
	}

	blocks := b.blocks[b.size:]
	b.blocks = b.blocks[0:b.size]

	return blocks, nil
}

func (b *BlockBuffer) handleUndo(block *pbsubstreams.BlockScopedData) error {
	if len(b.blocks) == 0 {
		return nil
	}

	for i := len(b.blocks) - 1; i >= 0; i-- {
		if b.blocks[i].Clock.Number >= block.Clock.Number {
			b.blocks = b.blocks[0:i]
		} else {
			break
		}
	}

	return nil
}

func (b *BlockBuffer) handleNew(block *pbsubstreams.BlockScopedData) error {
	b.blocks = append(b.blocks, block)

	sort.Slice(b.blocks, func(i, j int) bool {
		return b.blocks[i].Clock.Number < b.blocks[j].Clock.Number
	})

	return nil
}
