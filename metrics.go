package sink

import "github.com/streamingfast/dmetrics"

func RegisterMetrics() {
	metrics.Register()
}

var metrics = dmetrics.NewSet()

var HeadBlockNumber = metrics.NewHeadBlockNumber("substreams_sink")
var HeadBlockTimeDrift = metrics.NewHeadTimeDrift("substreams_sink")

var MessageSizeBytes = metrics.NewCounter("substreams_sink_message_size_bytes", "The number of total bytes of message received from the Substreams backend")

var SubstreamsErrorCount = metrics.NewCounter("substreams_sink_error", "The error count we encountered when interacting with Substreams for which we had to restart the connection loop")
var DataMessageCount = metrics.NewCounter("substreams_sink_data_message", "The number of data message received")
var DataMessageSizeBytes = metrics.NewCounter("substreams_sink_data_message_size_bytes", "The total size of in bytes of all data message received")
var ProgressMessageCount = metrics.NewCounterVec("substreams_sink_progress_message", []string{"module"}, "The number of progress message received")
var ProgressMessageLastEndBlock = metrics.NewGaugeVec("substreams_sink_progress_message_last_end_block", []string{"module"}, "Latest progress reported processed range end block for each module, usually increments but due scheduling could make that fluctuates up/down")
var UndoMessageCount = metrics.NewCounter("substreams_sink_undo_message", "The number of block undo message received")
var UnknownMessageCount = metrics.NewCounter("substreams_sink_unknown_message", "The number of unknown message received")

var BackprocessingCompletion = metrics.NewGauge("substreams_sink_backprocessing_completion", "Determines if backprocessing is completed, which is if we receive a first data message")
