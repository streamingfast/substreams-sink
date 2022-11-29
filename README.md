## Substreams Sink

This is a general purpose library used to help with the creation of specialized sinks for Substreams which save the output data to some kind of storage (eg: redis, mongodb, json files, etc). It is not intended to be used directly, but rather as a dependency for other libraries.

### Usage

The library provides a `Sinker` class that can be used to connect to the Substreams API. The `Sinker` class is a wrapper around the `substreams` library, which is a low-level library that provides a convenient way to connect to the Substreams API. 

The user's primary responsibility when creating a custom sink is to implement a `BlockScopeDataHandler` implementation which has the following interface:

```go
type BlockScopeDataHandler = func(ctx context.Context, cursor *Cursor, data *pbsubstreams.BlockScopedData) error
```
* `ctx`: the context that was used to retrieve the data.
* `cursor`: the cursor at the given block. This cursor should be saved regularly as a checkpoint in case the process is interrupted.
* `data`: `*pbsubstreams.BlockScopedData` parameter contains the data that was received from the Substreams API, and has the following fields: 

```go
type BlockScopedData struct {
	Outputs []*pbsubstreams.ModuleOutput 
	Clock   *pbsubstreams.Clock          
	Step    ForkStep        
	Cursor  string          
}
```

> note: pbsubstreams is the common alias for the `github.com/streamingfast/substreams/pb/sf/substreams/v1` package found [here](https://github.com/streamingfast/substreams/tree/develop/pb/sf/substreams/v1).


The `BlockScopeDataHandler` is called for each data message that is received from the Substreams API and contains all the data output for the given substreams module.

The `BlockScopeDataHandler` is responsible for decoding and processing the data, and returning an error if there is a problem. The `BlockScopeDataHandler` is also responsible for storing the cursor to the last processed block number. 

The basic pattern for using the `Sinker` is as follows:

* Create your data layer which is responsible for decoding the substreams' data and saving it to the desired storage.
* Have this object implement the `BlockScopeDataHandler` interface.
* Create a `Sinker` object using `sink.New` and pass in the `BlockScopeDataHandler` object.

### Launching

The sinker can be launched by calling the `Start` method on the `Sinker` object. The `Start` method will block until the sinker is stopped.

The sinker implements the [shutter](https://github.com/streamingfast/shutter/blob/develop/shutter.go) interface which can be used to handle all shutdown logic (eg: flushing any remaining data to storage, stopping the sink in case of database disconnection, etc.)

### Example uses

The following repositories are examples of how the sink library can be used:

* [substreams-sink-mongodb](https://github.com/streamingfast/substreams-sink-mongodb)
* [substreams-sink-postgres](https://github.com/streamingfast/substreams-sink-postgres)
* [substreams-sink-files](https://github.com/streamingfast/substreams-sink-files)