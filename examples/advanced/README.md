### Substreams Sink Examples - Advanced

This example is similar to [examples/basic](../basic/) with the difference that we use `sink.NewFromViper` to construct our instance which gives increase control on the sink for the operators in a "low-maintenance" fashion as the library deals with the flags and parsing. You will notice there is also less code to instantiate the `sink.Sinker` instance as more information is pulled from the flag.

Our example use our `github.com/streamingfast/cli` command definition which is just a thin wrapper around `cobra` and `viper`. Any `cobra` command should work with the library, there is no need to follow the CLI pattern.

> [!NOTE]
> We highly recommend to use this example for any serious sink implementation!

To run the example:

```bash
# We assume you are in ./examples/advanced
go run . sink mainnet.eth.streamingfast.io:443 https://github.com/streamingfast/substreams-eth-block-meta/releases/download/v0.5.1/substreams-eth-block-meta-v0.5.1.spkg db_out
```

Note also the availability of `go run . sink --help` which gives you most configuration option for free:

```bash
$ go run . sink --help
Run the sinker code

Usage:
  sinker sink <endpoint> <manifest> [<output_module>] [flags]

Flags:
      --development-mode                 Enable development mode, use it for testing purpose only, should not be used for production workload
      --final-blocks-only                Get only final blocks
  -H, --header stringArray               Additional headers to be sent in the substreams request
  -h, --help                             help for sink
      --infinite-retry                   Default behavior is to retry 15 times spanning approximatively 5m before exiting with an error, activating this flag will retry forever
  -k, --insecure                         Skip certificate validation on gRPC connection
      --irreversible-only                Get only irreversible blocks (DEPRECATED: Renamed to --final-blocks-only)
      --live-block-time-delta duration   Consider chain live if block time is within this number of seconds of current time (default 5m0s)
  -n, --network string                   Specify network, overriding the default one in the manifest or .spkg
  -p, --params -p <module>=<value>       Set a params for parameterizable modules of the from -p <module>=<value>, can be specified multiple times (e.g. -p module1=valA -p module2=valX&valY)
      --plaintext                        Establish gRPC connection in plaintext
      --skip-package-validation          Skip .spkg file validation, allowing the use of a partial spkg (without metadata and protobuf definitons)
      --undo-buffer-size int             Number of blocks to keep buffered to handle fork reorganizations (default 12)
```