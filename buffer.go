package sink

import (
	"fmt"
	"sort"
	"sync"

	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
)

type bufferKey string

func newBufferKey(blockNum uint64, blockId string, forkStep pbsubstreams.ForkStep) bufferKey {
	return bufferKey(fmt.Sprintf("%d-%s-%s", blockNum, blockId, forkStep))
}

type blockDataBuffer struct {
	size              int
	irrIdx            int
	lastBlockReturned uint64
	stopBlock         uint64

	index map[bufferKey]bool

	data []*pbsubstreams.BlockScopedData

	mu sync.RWMutex
}

func newBlockDataBuffer(size int) *blockDataBuffer {
	return &blockDataBuffer{
		size:  size,
		index: make(map[bufferKey]bool),
		data:  make([]*pbsubstreams.BlockScopedData, 0, size),
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

	return fmt.Errorf("unknown fork step %s", blockData.Step)
}

func (b *blockDataBuffer) GetBlockData() ([]*pbsubstreams.BlockScopedData, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	var blocks []*pbsubstreams.BlockScopedData

	if len(b.data) >= b.size {
		ix := len(b.data) - b.size
		if b.irrIdx > ix {
			ix = b.irrIdx
		}

		blocks = b.data[:ix]
		b.data = b.data[ix:]
		b.irrIdx = 0
	} else if b.irrIdx != 0 {
		blocks = b.data[0:b.irrIdx]
		b.data = b.data[b.irrIdx:]
		b.irrIdx = 0
	}

	if len(blocks) > 0 {
		b.lastBlockReturned = blocks[len(blocks)-1].Clock.Number
		go func() {
			for _, blk := range blocks {
				k := newBufferKey(blk.Clock.Number, blk.Clock.Id, blk.Step)
				delete(b.index, k)
			}
		}()
	}
	return blocks, nil
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
			k := newBufferKey(b.data[i].Clock.Number, b.data[i].Clock.Id, b.data[i].Step)
			delete(b.index, k)
			b.data = b.data[0:i]
		} else {
			break
		}
	}

	return nil
}

func (b *blockDataBuffer) handleNew(blockData *pbsubstreams.BlockScopedData) error {
	k := newBufferKey(blockData.Clock.Number, blockData.Clock.Id, blockData.Step)
	if _, ok := b.index[k]; ok {
		return nil
	}

	b.data = append(b.data, blockData)
	b.index[k] = true

	sort.Slice(b.data, func(i, j int) bool {
		return b.data[i].Clock.Number < b.data[j].Clock.Number
	})

	return nil
}

func (b *blockDataBuffer) handleIrreversible(blockData *pbsubstreams.BlockScopedData) error {
	k := newBufferKey(blockData.Clock.Number, blockData.Clock.Id, blockData.Step)
	if _, ok := b.index[k]; ok {
		return nil
	}

	b.data = append(b.data, blockData)
	b.irrIdx = len(b.data)
	b.index[k] = true

	sort.Slice(b.data, func(i, j int) bool {
		return b.data[i].Clock.Number < b.data[j].Clock.Number
	})

	return nil
}
