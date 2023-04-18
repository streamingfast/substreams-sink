package sink

import "github.com/streamingfast/dmetrics"

func RegisterMetrics() {
	metrics.Register()
}

var metrics = dmetrics.NewSet()

var HeadBlockNumber = metrics.NewHeadBlockNumber("substreams_sink")

var SubstreamsErrorCount = metrics.NewCounter("substreams_sink_error", "The error count we encountered when interacting with Substreams for which we had to restart the connection loop")
var DataMessageCount = metrics.NewCounter("substreams_sink_data_message", "The number of data message received")
var ProgressMessageCount = metrics.NewCounterVec("substreams_sink_progress_message", []string{"module"}, "The number of progress message received")
var UndoMessageCount = metrics.NewCounter("substreams_sink_undo_message", "The number of block undo message received")
