# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## v0.2.0

### Highlights

#### Update to `sf.substreams.rpc.v2` protocol and changed re-org handling

We have now updated the sink to use `sf.substreams.rpc.v2` protocol for communicating with the Substreams backend. This new protocol introduces a quite different way of receiving undo signal(s) from Substreams.

Each occurrences of `pbsubstreams.Request`, `pbsubstreams.Response`, `pbsubstreams.BlockScopedData` (and a few others) must be moved to `pbsubstreamsrpc.<Name>` where import statement is `pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"`. Note that `pbsubstreams.ForkStep` has been removed completely, below we discuss how to determine final block height.

You must now pass a `sink.BlockUndoSignalHandler` when running your `sink.Sinker` instance, this new handler will receive undo signal which are now just contains the last canonical block to revert back to and the cursor to use.

While previously you would receive steps like:

1. `BlockScopedData` (Step: `New`, Block #5a, Cursor `a`)
1. `BlockScopedData` (Step: `New`, Block #6b, Cursor `b`)
1. `BlockScopedData` (Step: `New`, Block #7b, Cursor `c`)
1. `BlockScopedData` (Step: `Undo`, Block #7b, Cursor `c`)
1. `BlockScopedData` (Step: `Undo`, Block #6b, Cursor `b`)
1. `BlockScopedData` (Step: `New`, Block #6a, Cursor `e`)

Now the signalling will be like:

1. `BlockScopedData` (Block #5a, Cursor `a`)
1. `BlockScopedData` (Block #6b, Cursor `b`)
1. `BlockScopedData` (Block #7b, Cursor `c`)
1. `BlockUndoSignal` (Block #5a, Cursor `a'`)
1. `BlockScopedData` (Block #6a, Cursor `e`)

Now a `BlockUndoSignal` must be treated as "delete every data that has been recorded after block height specified by block in BlockUndoSignal". In the example above, this means we must delete changes done by `Block #7b` and `Block #6b`. The exact details depends on your own logic. If for example all your added record contain a block number, a simple way is to do `delete all records where block_num > 5` which is the block num received in the `BlockUndoSignal` (this is true for append only records, so when only `INSERT` are allowed).

The `pbsubstreams.ForkStep` has been removed completely. The default behavior is to send `pbsubstreamsrpc.BlockScopedData` and `pbsubstreamsprc.BlockUndoSignal` which corresponds to old `ForkStepNew` and `ForkStepUndo`. The `ForkStepIrreversible` are not sent anymore. Instead, the `pbsubstreamsrpc.BlockScopedData` message gained a filed `FinalBlockHeight` which determines for the received block at which block height Substreams is considering blocks to be final. If you wish to only receive final blocks, you can use `sink.WithFinalBlocksOnly` sinker `Option`.

The `sink.Sinker` and `sink.New(...)` change in signature also to make nesting easier. Now the handlers must be passed when calling `Run`. The constructor also removed the possibility to pass which `ForkStep` to handle.

The `sink.BlockScopedDataHandler` signature changed, the message is now the first argument, the second argument determines if the block is live, it will be non-nil if a `sink.WithLivenessChecker` has been configured. If the liveness checker determines the block is live, current implementation checks if block's timestamp is within defined delta, the `isLive` will be pointing to a non-nil `true` value, otherwise a non-nil `false` value. Finally the cursor is the last element.

The `sink.BlockUndoSignalHandler` must be defined to correctly received undo signal from the Substreams RPC. If can be left `nil` only if `sink.WithFinalBlocksOnly` is configured. How you handle the undo signal is left on the consumer.

##### Before

```go
handler := func(ctx context.Context, cursor *sink.Cursor, data *pbsubstreams.BlockScopedData) error { ... }
steps := []pbsubstreams.ForkStep{pbsubstreams.ForkStep_STEP_NEW, pbsubstreams.ForkStep_STEP_UNDO}

sinkOptions := []sink.Option{...}
// If you had `steps := []pbsubstreams.ForkStep{pbsubstreams.ForkStep_STEP_IRREVERSIBLE}`, now pass `sink.WithFinalBlocksOnly` as a `SinkOption` instead
// sinkOptions := append(sinkOptions, sink.WithFinalBlocksOnly())

sink, err = sink.New(
	mode,
	s.Pkg.Modules,
	s.OutputModule,
	s.OutputModuleHash,
	s.handleBlockScopeData,
	s.ClientConfig,
	steps,
	s.logger,
	s.tracer,
	sinkOptions...,
)
if err != nil { ... }

if err := s.sink.Start(ctx, s.blockRange, cursor); err != nil {
    return fmt.Errorf("sink failed: %w", err)
}
```

##### After

```go
handleData := func(ctx context.Context, data *pbsubstreamsrpc.BlockScopedData, isLive *bool, cursor *Cursor) error { ... }
handleUndo := func(ctx context.Context, undo *pbsubstreamsrpc.BlockUndoSignal, cursor *Cursor) error { ... }

sinkOptions := []sink.Option{sink.WithBlockRange(s.blockRange)}
// If you had `steps := []pbsubstreams.ForkStep{pbsubstreams.ForkStep_STEP_IRREVERSIBLE}`, now pass `sink.WithFinalBlocksOnly` as a `SinkOption` instead
// sinkOptions := append(sinkOptions, sink.WithFinalBlocksOnly())

sink, err = sink.New(
	mode,
	s.Pkg.Modules,
	s.OutputModule,
	s.OutputModuleHash,
	s.ClientConfig,
	s.logger,
	s.tracer,
	sinkOptions...,
)
if err != nil { ... }

sink.OnTerminating(func (err error) {
    if err != nil {
        // Sinker failed, do something with err
    }
})

go sink.Run(ctx, cursor, sink.NewSinkerHandlers(handleData, handleUndo))
```

#### Liveness Checker

The `sink.Sinker` library added support for checking if a block is live or not. You can configure our `sink.DeltaLivenessChecker` check passing it as an `Option` on `sink.New(..., sink.WithLivenessChecker(sink.NewDeltaLivenessChecker(delta)))`. The `sink.DeltaLivenessChecker` determines that a block is live is `time.Now() - block.Timestamp() <= delta`.

Once a liveness checker is configured, the `isLive` argument in your `sink.BlockScopedDataHandler` will start to be non-nil and the value will be result of having called `sink.DeltaLivenessChecker.IsLive(block)`.

#### Retryable Errors

Errors coming out of the handler(s) are not retried by default anymore. This because errors coming out of your handler are usually not retryable. If you wish to keep this behavior, you can use `sink.NewRetryableError(err)` to make it back retryable.

#### Cursor

The `sink.Cursor` backing implementation changed which will now avoid the need to pass to which block the cursor points to. The block pointed to is now extracted directly from the cursor value.

- `sink.NewCursor(cursor, block)` becomes simply `sink.NewCursor(cursor)`.

### Added

- Added `sink.NewFromViper` to easily create an instance from viper predefined flags, expected that `cli.ConfigureViper` or `cli.ConfigureViperForCommand` has been used, use `sink.AddFlagsToSet` to add flags to a flag set.
    - This new method make it much easier to maintain the a sink flags and configures it from flags. This changelog will list changes made to flags so when updating, you can copy it over to your own changelog.

- Added `sink.AddFlagsToSet` to easily add all sinker flags to a `pflag.FlagSet` instance.

- Added `sink.WithBlockRange` sinker `Option` to limit the `Sinker` to a specific range (runs for whole chain if unset).

- Added `sink.WithLivenessChecker` sinker `Option` to configure a liveness check on the sinker instance.

### Changed

- Stats are printed each 15s when logger level is info or higher and 5s when it's debug or lower.

- **Deprecation** The flag `--irreversible-only` is deprecated, use `--final-blocks-only` instead.

- **Breaking** The `sink.New` signature changed, handlers

- **Breaking** The `sink.New` signature changed
    - The `sink.BlockScopedDataHandler` handler must not be passed in the constructor anymore.
    - The `forkSteps` arguments has been removed.

- **Breaking** The `sink.Sinker` field `BlockScopedDataHandler` has been removed (renamed and made private).

- **Breaking** Type name `sink.BlockScopeDataHandler` signature changed, the argument `data` is now of type `pbsubstreamsrpc.BlockScopedData` (imported via `pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"`).

## v0.1.0

- **Breaking** Type name `sink.BlockScopeDataHandler` has been renamed to `sink.BlockScopedDataHandler` (note the `d` on `Scoped`).
First official release of the library, latest release until refactor to support the new upcoming Substreams V2 RPC protocol.
