package sink

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/streamingfast/bstream"
	"github.com/streamingfast/cli/sflags"
	"github.com/streamingfast/logging"
	"github.com/streamingfast/substreams/client"
	"github.com/streamingfast/substreams/manifest"
	pbsubstreams "github.com/streamingfast/substreams/pb/sf/substreams/v1"
	"go.uber.org/zap"
)

// AddFlagsToSet can be used to import standard flags needed for sink to configure itself. By using
// this method to define your flag and using `cli.ConfigureViper` (import "github.com/streamingfast/cli")
// in your main application command, `NewFromViper` is usable to easily create a `sink.Sinker` instance.
//
// Defines
//
//	Flag `--insecure` (-k) (defaults `false`)
//	Flag `--plaintext` (-p) (defaults `false`)
//	Flag `--undo-buffer-size` (defaults `12`)
//	Flag `--live-block-time-delta` (defaults `300*time.Second`)
//	Flag `--development-mode` (defaults `false`)
//	Flag `--final-blocks-only` (defaults `false`)
//	Flag `--infinite-retry` (defaults `false`)
func AddFlagsToSet(flags *pflag.FlagSet) {
	flags.BoolP("insecure", "k", false, "Skip certificate validation on GRPC connection")
	flags.BoolP("plaintext", "p", false, "Establish GRPC connection in plaintext")
	flags.Int("undo-buffer-size", 12, "Number of blocks to keep buffered to handle fork reorganizations")
	flags.Duration("live-block-time-delta", 300*time.Second, "Consider chain live if block time is within this number of seconds of current time")
	flags.Bool("development-mode", false, "Enable development mode, use it for testing purpose only, should not be used for production workload")
	flags.Bool("final-blocks-only", false, "Get only final blocks")
	flags.Bool("infinite-retry", false, "Default behavior is to retry 15 times spanning approximatively 5m before exiting with an error, activating this flag will retry forever")

	// Deprecated flags
	flags.Bool("irreversible-only", false, "Get only irreversible blocks")
	flags.Lookup("irreversible-only").Deprecated = "Renamed to --final-blocks-only"
}

// NewFromViper constructs a new Sinker instance from a fixed set of "known" flags.
//
// If you want to extract the sink output module's name directly from the Substreams
// package, if supported by your sink, instead of an actual name for paramater
// `outputModuleNameArg`, use `sink.InferOutputModuleFromPackage`.
//
// The `expectedOutputModuleType` should be the fully qualified expected Protobuf
// package
func NewFromViper(
	cmd *cobra.Command,
	expectedOutputModuleType string,
	endpoint, manifestPath, outputModuleName, blockRange string,
	zlog *zap.Logger,
	tracer logging.Tracer,
	opts ...Option,
) (*Sinker, error) {
	zlog.Info("sinker from CLI",
		zap.String("endpoint", endpoint),
		zap.String("manifest_path", manifestPath),
		zap.String("output_module_name", outputModuleName),
		zap.String("expected_module_type", expectedOutputModuleType),
		zap.String("block_range", blockRange),
	)

	zlog.Info("reading substreams manifest", zap.String("manifest_path", manifestPath))
	pkg, err := manifest.NewReader(manifestPath).Read()
	if err != nil {
		return nil, fmt.Errorf("read manifest: %w", err)
	}

	graph, err := manifest.NewModuleGraph(pkg.Modules.Modules)
	if err != nil {
		return nil, fmt.Errorf("create substreams moduel graph: %w", err)
	}

	resolvedOutputModuleName := outputModuleName
	if resolvedOutputModuleName == InferOutputModuleFromPackage {
		zlog.Debug("inferring module output name from package directly")
		if pkg.SinkModule == "" {
			return nil, fmt.Errorf("sink module is required in sink config")
		}

		resolvedOutputModuleName = pkg.SinkModule
	}

	zlog.Info("validating output module", zap.String("module_name", resolvedOutputModuleName))
	module, err := graph.Module(resolvedOutputModuleName)
	if err != nil {
		return nil, fmt.Errorf("get output module %q: %w", resolvedOutputModuleName, err)
	}
	if module.GetKindMap() == nil {
		return nil, fmt.Errorf("ouput module %q is *not* of  type 'Mapper'", resolvedOutputModuleName)
	}

	zlog.Info("validating output module type", zap.String("module_name", module.Name), zap.String("module_type", module.Output.Type))

	unprefixedExpectedType, prefixedExpectedType := sanitizeModuleType(expectedOutputModuleType)
	unprefixedActualType, prefixedActualType := sanitizeModuleType(module.Output.Type)
	if prefixedActualType != prefixedExpectedType {
		return nil, fmt.Errorf("sink only supports map module with output type %q but selected module %q output type is %q", unprefixedExpectedType, module.Name, unprefixedActualType)
	}

	hashes := manifest.NewModuleHashes()
	outputModuleHash := hashes.HashModule(pkg.Modules, module, graph)

	apiToken := readAPIToken(cmd)
	resolvedBlockRange, err := readBlockRange(module, blockRange)
	if err != nil {
		return nil, fmt.Errorf("resolve block range: %w", err)
	}

	zlog.Debug("resolved block range", zap.Stringer("range", resolvedBlockRange))

	undoBufferSize := sflags.MustGetInt(cmd, "undo-buffer-size")
	liveBlockTimeDelta := sflags.MustGetDuration(cmd, "live-block-time-delta")
	isDevelopmentMode := sflags.MustGetBool(cmd, "development-mode")
	infiniteRetry := sflags.MustGetBool(cmd, "infinite-retry")

	finalBlocksOnly, isSet := sflags.MustGetBoolProvided(cmd, "final-blocks-only")
	if !isSet {
		finalBlocksOnly = sflags.MustGetBool(cmd, "irreversible-only")
	}

	clientConfig := client.NewSubstreamsClientConfig(
		endpoint,
		apiToken,
		sflags.MustGetBool(cmd, "insecure"),
		sflags.MustGetBool(cmd, "plaintext"),
	)

	mode := SubstreamsModeProduction
	if isDevelopmentMode {
		mode = SubstreamsModeDevelopment
	}

	var defaultSinkOptions []Option
	if undoBufferSize > 0 {
		defaultSinkOptions = append(defaultSinkOptions, WithBlockDataBuffer(undoBufferSize))
	}

	if infiniteRetry {
		defaultSinkOptions = append(defaultSinkOptions, WithInfiniteRetry())
	}

	if liveBlockTimeDelta > 0 {
		defaultSinkOptions = append(defaultSinkOptions, WithLivenessChecker(NewDeltaLivenessChecker(liveBlockTimeDelta)))
	}

	if finalBlocksOnly {
		defaultSinkOptions = append(defaultSinkOptions, WithFinalBlocksOnly())
	}

	if resolvedBlockRange != nil {
		defaultSinkOptions = append(defaultSinkOptions, WithBlockRange(resolvedBlockRange))
	}

	return New(
		mode,
		pkg,
		module,
		outputModuleHash,
		clientConfig,
		zlog,
		tracer,
		append(defaultSinkOptions, opts...)...,
	)
}

// parseNumber parses a number and indicates whether the number is relative, meaning it starts with a +
func parseNumber(number string) (int64, bool, error) {
	numberIsRelative := strings.HasPrefix(number, "+")
	numberInt64, err := strconv.ParseInt(strings.TrimPrefix(number, "+"), 0, 64)
	if err != nil {
		return 0, false, fmt.Errorf("invalid block number value: %w", err)
	}
	return numberInt64, numberIsRelative, nil
}

func readBlockRange(module *pbsubstreams.Module, input string) (*bstream.Range, error) {
	if input == "" {
		input = "-1"
	}

	before, after, rangeHasStartAndStop := strings.Cut(input, ":")

	beforeAsInt64, beforeIsRelative, err := parseNumber(before)
	if err != nil {
		return nil, fmt.Errorf("parse number %q: %w", before, err)
	}

	afterIsRelative := false
	afterAsInt64 := int64(0)
	if rangeHasStartAndStop {
		afterAsInt64, afterIsRelative, err = parseNumber(after)
		if err != nil {
			return nil, fmt.Errorf("parse number %q: %w", after, err)
		}

	}

	// If there is no `:` we assume it's a stop block value right away
	if !rangeHasStartAndStop {
		if beforeAsInt64 < 1 {
			return bstream.NewOpenRange(module.InitialBlock), nil
		}
		start := module.InitialBlock
		stop := resolveBlockNumber(beforeAsInt64, 0, beforeIsRelative, int64(start))
		return bstream.NewRangeExcludingEnd(start, uint64(stop)), nil
	}

	start := resolveBlockNumber(beforeAsInt64, int64(module.InitialBlock), beforeIsRelative, int64(module.InitialBlock))
	if afterAsInt64 == -1 {
		return bstream.NewOpenRange(uint64(start)), nil
	}

	return bstream.NewRangeExcludingEnd(uint64(start), uint64(resolveBlockNumber(afterAsInt64, 0, afterIsRelative, start))), nil
}

func resolveBlockNumber(value int64, defaultIfNegative int64, relative bool, against int64) int64 {
	if !relative {
		if value < 0 {
			return defaultIfNegative
		}
		return value
	}
	return int64(against) + value
}

func readAPIToken(cmd *cobra.Command) string {
	apiToken := os.Getenv("SUBSTREAMS_API_TOKEN")
	if apiToken != "" {
		return apiToken
	}

	return os.Getenv("SF_API_TOKEN")
}

// sanitizeModuleType give back both prefixed (so with `proto:`) and unprefixed
// version of the input string:
//
// - `sanitizeModuleType("com.acme") == (com.acme, proto:com.acme)`
// - `sanitizeModuleType("proto:com.acme") == (com.acme, proto:com.acme)`
func sanitizeModuleType(in string) (unprefixed, prefixed string) {
	if strings.HasPrefix("proto:", in) {
		return strings.TrimPrefix(in, "proto:"), in
	}

	return in, "proto:" + in
}
