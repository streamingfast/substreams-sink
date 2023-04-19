package sink

import (
	"context"
	"fmt"

	"github.com/streamingfast/bstream"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
	"go.uber.org/zap/zapcore"
)

func NewSinkerHandlers(
	handleBlockScopedData func(ctx context.Context, data *pbsubstreamsrpc.BlockScopedData, isLive *bool, cursor *Cursor) error,
	handleBlockUndoSignal func(ctx context.Context, undoSignal *pbsubstreamsrpc.BlockUndoSignal, cursor *Cursor) error,
) SinkerHandlers {
	return SinkerHandlers{
		HandleBlockScopedData: handleBlockScopedData,
		HandleBlockUndoSignal: handleBlockUndoSignal,
	}
}

type SinkerHandlers struct {
	// HandleBlockScopedData defines the callback that will handle Substreams `BlockScopedData` messages.
	//
	// The handler receives the following arguments:
	// - `ctx` is the context runtime, your handler should be minimal, so normally you shouldn't use this.
	// - `data` contains the block scoped data that was received from the Substreams API, refer to it's definition for proper usage.
	// - `isLive` will be non-nil if a [LivenessChecker] has been configured on the [Sinker] instance that call the handler.
	// - `cursor` is the cursor at the given block, this cursor should be saved regularly as a checkpoint in case the process is interrupted.
	//
	// The [HandleBlockScopedData] must be non-nil, the [Sinker] enforces this.
	//
	// Your handler must return an error value that can be nil or non-nil. If non-nil, the error is assumed to be a fatal
	// error and the [Sinker] will not retry it. If the error is retryable, wrap it in `sink.NewRetryableError(err)` to notify
	// the [Sinker] that it should retry from last valid cursor. It's your responsibility to ensure no data was persisted prior the
	// the error.
	HandleBlockScopedData func(ctx context.Context, data *pbsubstreamsrpc.BlockScopedData, isLive *bool, cursor *Cursor) error

	// HandleBlockUndoSignal defines the callback that will handle Substreams `BlockUndoSignal` messages.
	//
	// The handler receives the following arguments:
	// - `ctx` is the context runtime, your handler should be minimal, so normally you shouldn't use this.
	// - `undoSignal` contains the last valid block that is still valid, any data saved after this last saved block should be discarded.
	// - `cursor` is the cursor at the given block, this cursor should be saved regularly as a checkpoint in case the process is interrupted.
	//
	// The [HandleBlockUndoSignal] can be nil if the sinker is configured to stream final blocks only, otherwise it must be set,
	// the [Sinker] enforces this.
	//
	// Your handler must return an error value that can be nil or non-nil. If non-nil, the error is assumed to be a fatal
	// error and the [Sinker] will not retry it. If the error is retryable, wrap it in `sink.NewRetryableError(err)` to notify
	// the [Sinker] that it should retry from last valid cursor. It's your responsibility to ensure no data was persisted prior the
	// the error.
	HandleBlockUndoSignal func(ctx context.Context, undoSignal *pbsubstreamsrpc.BlockUndoSignal, cursor *Cursor) error
}

func (s SinkerHandlers) String() string {
	dataHandlerState := "defined"
	if s.HandleBlockScopedData == nil {
		dataHandlerState = "undefined"
	}

	undoHandlerState := "defined"
	if s.HandleBlockUndoSignal == nil {
		undoHandlerState = "undefined"
	}

	return fmt.Sprintf("HandleBlockScopedData: %s, HandleBlockUndoSignal: %s", dataHandlerState, undoHandlerState)
}

type Cursor bstream.Cursor

func NewCursor(cursor string) (*Cursor, error) {
	if cursor == "" {
		return blankCursor, nil
	}

	decoded, err := bstream.CursorFromOpaque(cursor)
	if err != nil {
		return nil, fmt.Errorf("decode %q: %w", cursor, err)
	}

	return (*Cursor)(decoded), nil
}

func MustNewCursor(cursor string) *Cursor {
	decoded, err := NewCursor(cursor)
	if err != nil {
		panic(err)
	}

	return decoded
}

var blankCursor = (*Cursor)((*bstream.Cursor)(nil))

func NewBlankCursor() *Cursor {
	return blankCursor
}

func (c *Cursor) IsBlank() bool {
	return c == nil || c == blankCursor
}

func (c *Cursor) IsEqualTo(other *Cursor) bool {
	if c.IsBlank() && other.IsBlank() {
		return true
	}

	// We know both are not equal, so if either side is `nil`, we are sure the other is not, so not equal
	if !c.IsBlank() || !other.IsBlank() {
		return false
	}

	// Both side are non-nil here
	actual := (*bstream.Cursor)(c)
	candidate := (*bstream.Cursor)(other)

	return actual.Equals(candidate)
}

func (c *Cursor) MarshalLogObject(encoder zapcore.ObjectEncoder) error {
	cursor := "<Blank>"
	if !c.IsBlank() {
		cursor = c.String()
	}

	encoder.AddString("cursor", cursor)
	return nil
}

// String returns a string representation suitable for handling a Firehose request
// meaning a blank cursor returns "".
func (c *Cursor) String() string {
	if c.IsBlank() {
		return ""
	}

	return (*bstream.Cursor)(c).ToOpaque()
}

//go:generate go-enum -f=$GOFILE --marshal --names

// ENUM(
//
//	Development
//	Production
//
// )
type SubstreamsMode uint
