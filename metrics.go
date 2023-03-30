package sink

import "github.com/streamingfast/dmetrics"

func RegisterMetrics() {
	metrics.Register()
}

var metrics = dmetrics.NewSet()

var SubstreamsErrorCount = metrics.NewCounter("substreams_sink_error", "The error count we encountered when interacting with Substreams for which we had to restart the connection loop")
var DataMessageCount = metrics.NewCounterVec("substreams_sink_data_message", []string{"module"}, "The number of data message received")
var ProgressMessageCount = metrics.NewCounterVec("substreams_sink_progress_message", []string{"module"}, "The number of progress message received")
var BlockCount = metrics.NewCounter("substreams_sink_block_count", "The number of data received")
var HeadBlockNumber = metrics.NewHeadBlockNumber("substreams_sink")
