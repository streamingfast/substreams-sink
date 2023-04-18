# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Highlights

#### Update to `sf.substreams.rpc.v2` protocol and changed re-org handling

We have now updated the sink to use `sf.substreams.rpc.v2` protocol for communicating with the Substreams backend. This new protocol introduce a quite different way of receiving undo signal from Substreams.

You must now pass a `sink.BlockUndoSignalHandler` when constructing `sink.New`, this handler will receive undo signal which are now just contains the last canonical block to revert back to and the cursor to use.

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

Now a `BlockUndoSignal` must be treated as "delete every data that has been recorded after block height specified by block in BlockUndoSignal". In the example above, this means we must delete changes done by `Block #7b` and `Block #6b`. The exact details depends on your own logic. If for example all your added record contain a block number, a simple way is to do `delete all records where block_num > 5` which is the block num received in the `BlockUndoSignal`.

### Changed

- **Deprecated** The flag `--irreversible-only` is now deprecated, use `--final-blocks-only` instead.

- The `sink.New` signature changed, just after the `sink.BlockScopeDataHandler` you must pass a `sink.BlockUndoSignalHandler` that will now receive "undo" signals.

- **Breaking** Struct field `BlockScopedDataHandler` from `sink.Sinker` has been removed (renamed and made private).

- **Breaking** Type name `sink.BlockScopeDataHandler` signature changed, the argument `data` is now of type `pbsubstreamsrpc.BlockScopedData` (imported via `pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"`).

  This drastically change how undo signal must be processed they are now emitted via a new

- **Breaking** Type name `sink.BlockScopeDataHandler` has been renamed to `sink.BlockScopedDataHandler` (note the `d` on `Scoped`).
