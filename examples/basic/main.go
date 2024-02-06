package main

import (
	"context"
	"fmt"
	"os"

	"github.com/streamingfast/cli"
	"github.com/streamingfast/logging"
	sink "github.com/streamingfast/substreams-sink"
	pbchanges "github.com/streamingfast/substreams-sink-database-changes/pb/sf/substreams/sink/database/v1"
	"github.com/streamingfast/substreams/client"
	pbsubstreamsrpc "github.com/streamingfast/substreams/pb/sf/substreams/rpc/v2"
)

var expectedOutputModuleType = string(new(pbchanges.DatabaseChanges).ProtoReflect().Descriptor().FullName())

var zlog, tracer = logging.RootLogger("project", "github.com/change_to_org/change_to_project")

func main() {
	logging.InstantiateLoggers()

	cli.Ensure(len(os.Args) == 3 || len(os.Args) == 4, "usage: %s <endpoint> <manifest> [<output_module>]", os.Args[0])

	endpoint := os.Args[1]
	manifestPath := os.Args[2]

	// Find the output module in the manifest sink.moduleName configuration. If you have no
	// such configuration, you can change the value below and set the module name explicitly.
	outputModuleName := sink.InferOutputModuleFromPackage
	if len(os.Args) == 4 {
		outputModuleName = os.Args[3]
	}

	// We do not use params in this example, but if you have some, you would define it here.
	// Each param is of the form `module=value` or `imported:module=value`.
	params := ([]string)(nil)

	pkg, outputModule, outputHashModule, blockRange, err := sink.ReadManifestAndModuleAndBlockRange(
		manifestPath,
		// Equivalent to the "network" parameter of the CLI. Specifies the network to use 
		// for params and initialBlocks, overriding the "network" field in the Substreams package.
		"mainnet",
		params,
		outputModuleName,
		// Should be the Protobuf full name of the map's module output, we use
		// `substreams-database-changes` imported type. Adjust to your needs.
		//
		// If your Protobuf is defined in your Substreams manifest, you can use `substream protogen`
		// while being in the same folder that contain `buf.gen.yaml` file in the example folder.
		expectedOutputModuleType,
		false,
		// This is the block range, in our case defined as Substreams module's start block and up forever
		":",
		zlog,
	)
	cli.NoError(err, "unable to read manifest: %s", err)

	sinker, err := sink.New(
		sink.SubstreamsModeProduction,
		pkg,
		outputModule,
		outputHashModule,
		// Deals with endpoint, API token and plaintext/TLS configuration
		client.NewSubstreamsClientConfig(endpoint, os.Getenv("SUBSTREAMS_API_TOKEN"), false, false),
		zlog,
		tracer,
		sink.WithBlockRange(blockRange),
	)
	cli.NoError(err, "unable to create sinker: %s", err)

	sinker.OnTerminating(func(err error) {
		cli.NoError(err, "unexpected sinker error")

		zlog.Info("sink is terminating")
	})

	// You **must** save the cursor somewhere, saving it to memory while
	// make it last until the process is killed, in which on re-start, the
	// sinker will resume from start block again. You can simply read from
	// a file the string value of the cursor and use `sink.NewCursor(value)`
	// to load it.

	sinker.Run(context.Background(), sink.NewBlankCursor(), sink.NewSinkerHandlers(handleBlockScopedData, handleBlockUndoSignal))
}

func handleBlockScopedData(ctx context.Context, data *pbsubstreamsrpc.BlockScopedData, isLive *bool, cursor *sink.Cursor) error {
	_ = ctx

	changes := &pbchanges.DatabaseChanges{}
	if err := data.Output.MapOutput.UnmarshalTo(changes); err != nil {
		return fmt.Errorf("unable to unmarshal database changes: %w", err)
	}

	fmt.Printf("Block #%d (%s) data received with %d changes\n", data.Clock.Number, data.Clock.Id, len(changes.TableChanges))

	// Once you have processed the block, you **must** persist the cursor, persistence
	// can take differnet form. For example, you can save it to a file, or to a database.
	// You can simply use `os.WriteFile("cursor.txt", []byte(cursor.String()), 0644)` to
	// save it to a file.
	_ = cursor

	// The isLive boolean is set to true when the sinker is running in the live portion
	// of the chain. If there is **no** liveness checker configured on the sinker, the isLive
	// value is going to be nil.
	_ = isLive

	return nil
}

func handleBlockUndoSignal(ctx context.Context, undoSignal *pbsubstreamsrpc.BlockUndoSignal, cursor *sink.Cursor) error {
	_ = ctx

	// The chain forked for one or more blocks, you **must** rewind your changes back to the
	// last valid block, which is provided in the undo signal. You **must** also persist the
	// last valid cursor also provided in the undo signal. You can simply use
	// `os.WriteFile("cursor.txt", []byte(cursor.String()), 0644)` to
	// save it to a file.
	_ = cursor

	fmt.Printf("Rewinding changes back to block %s\n", undoSignal.LastValidBlock)
	return nil
}
