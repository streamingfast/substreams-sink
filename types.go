package sink

import (
	"context"
	"github.com/streamingfast/bstream"
	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
)

type BlockScopeDataHandler = func(ctx context.Context, cursor *Cursor, data *pbsubstreams.BlockScopedData) error

type Cursor struct {
	Cursor string
	Block  bstream.BlockRef
}

func NewCursor(cursor string, block bstream.BlockRef) *Cursor {
	return &Cursor{cursor, block}
}

func NewBlankCursor() *Cursor {
	return NewCursor("", bstream.BlockRefEmpty)
}

func (c *Cursor) IsBlank() bool {
	return c.Cursor == ""
}
