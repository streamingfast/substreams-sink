package sink

import (
	"runtime"
	"testing"
	"time"

	"go.uber.org/zap"
)

// TestStatsClose_DoesNotLeakGoroutines verifies that Stats.Close stops every
// background goroutine it spawned. Each AvgRate* helper constructed by
// newStats runs a janitor goroutine; if Close forgets to stop any of them, a
// long-running consumer that creates a fresh Stats per iteration will leak
// goroutines monotonically (see redpandasink continuous mode).
func TestStatsClose_DoesNotLeakGoroutines(t *testing.T) {
	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	baseline := runtime.NumGoroutine()

	const iterations = 50
	for i := 0; i < iterations; i++ {
		s := newStats(zap.NewNop())
		s.Close()
	}

	runtime.GC()
	time.Sleep(200 * time.Millisecond)
	growth := runtime.NumGoroutine() - baseline

	// Two-goroutine tolerance covers ordinary scheduler/runtime noise. A bug
	// where Close forgets one janitor produces growth ~= iterations, which is
	// well outside the tolerance.
	const tolerance = 2
	if growth > tolerance {
		t.Fatalf("Stats.Close leaks goroutines: grew by %d after %d iterations (tolerance=%d)", growth, iterations, tolerance)
	}
}
