package sink

import "fmt"

type AverageInt64 struct {
	name       string
	entries    []int64
	sum        int64
	Average    float64
	entryCount int
}

func NewAverageInt64(name string) *AverageInt64 {
	return &AverageInt64{name: name, entryCount: 100}
}

func NewAverageInt64WithCount(name string, entryCount int) *AverageInt64 {
	return &AverageInt64{name: name, entryCount: entryCount}
}

func (a *AverageInt64) Add(value int64) {
	a.entries = append(a.entries, value)
	a.sum += value

	if len(a.entries) == a.entryCount+1 {
		var first int64
		first, a.entries = a.entries[0], a.entries[1:]
		a.sum -= first
	}
	a.Average = float64(a.sum) / float64(len(a.entries))
}

func (a *AverageInt64) Entries() int {
	return len(a.entries)
}

func (a *AverageInt64) Reset() {
	a.entries = nil
	a.sum = 0
	a.Average = 0
}

func (a *AverageInt64) String() string {
	return fmt.Sprintf("%s: %f", a.name, a.Average)
}
