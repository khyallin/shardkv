package group

import (
	"sync"
	"time"

	"github.com/khyallin/shardkv/api"
)

const Size = 60

type metricsBucket struct {
	sec          int64
	ingress      int64
	done         int64
	success      int64
	latencySumNs int64
	latencyMaxNs int64
}

type Metrics struct {
	mu      sync.Mutex
	buckets [Size]metricsBucket
}

func MakeMetrics() *Metrics {
	return &Metrics{}
}

func (m *Metrics) OnIngress() time.Time {
	now := time.Now()
	nowSec := now.Unix()

	m.mu.Lock()
	b := m.bucket(nowSec)
	b.ingress++
	m.mu.Unlock()
	return now
}

func (m *Metrics) OnDone(start time.Time, err api.Err) {
	now := time.Now()
	nowSec := now.Unix()
	latencyNs := now.Sub(start).Nanoseconds()
	if latencyNs < 0 {
		latencyNs = 0
	}

	m.mu.Lock()
	b := m.bucket(nowSec)
	b.done++
	if err == api.OK {
		b.success++
	}
	b.latencySumNs += latencyNs
	if latencyNs > b.latencyMaxNs {
		b.latencyMaxNs = latencyNs
	}
	m.mu.Unlock()
}

func (m *Metrics) View() *MetricsView {
	nowSec := time.Now().Unix()

	m.mu.Lock()
	view := &MetricsView{
		nowSec:   nowSec,
		buckets:  m.buckets,
	}
	m.mu.Unlock()
	return view
}

type MetricsView struct {
	nowSec   int64
	buckets  [Size]metricsBucket
}

func (v *MetricsView) TotalQPS(seconds int) float64 {
	seconds = v.normalizeWindow(seconds)
	total := v.sum(seconds, func(b metricsBucket) int64 { return b.ingress })
	return float64(total) / float64(seconds)
}

func (v *MetricsView) DoneQPS(seconds int) float64 {
	seconds = v.normalizeWindow(seconds)
	total := v.sum(seconds, func(b metricsBucket) int64 { return b.done })
	return float64(total) / float64(seconds)
}

func (v *MetricsView) SuccessQPS(seconds int) float64 {
	seconds = v.normalizeWindow(seconds)
	total := v.sum(seconds, func(b metricsBucket) int64 { return b.success })
	return float64(total) / float64(seconds)
}

func (v *MetricsView) MaxLatency(seconds int) time.Duration {
	seconds = v.normalizeWindow(seconds)
	var maxNs int64
	for _, b := range v.recentBuckets(seconds) {
		if b.latencyMaxNs > maxNs {
			maxNs = b.latencyMaxNs
		}
	}
	return time.Duration(maxNs)
}

func (v *MetricsView) AvgLatency(seconds int) time.Duration {
	seconds = v.normalizeWindow(seconds)
	var totalLatency int64
	var doneCount int64
	for _, b := range v.recentBuckets(seconds) {
		totalLatency += b.latencySumNs
		doneCount += b.done
	}
	if doneCount == 0 {
		return 0
	}
	return time.Duration(totalLatency / doneCount)
}

func (m *Metrics) bucket(sec int64) *metricsBucket {
	idx := int(sec % Size)
	b := &m.buckets[idx]
	if b.sec != sec {
		*b = metricsBucket{sec: sec}
	}
	return b
}

func (v *MetricsView) normalizeWindow(seconds int) int {
	if seconds < 1 {
		return 1
	}
	if seconds > Size {
		return Size
	}
	return seconds
}

func (v *MetricsView) recentBuckets(seconds int) []metricsBucket {
	startSec := v.nowSec - int64(seconds) + 1
	result := make([]metricsBucket, 0, seconds)
	for _, b := range v.buckets {
		if b.sec >= startSec && b.sec <= v.nowSec {
			result = append(result, b)
		}
	}
	return result
}

func (v *MetricsView) sum(seconds int, field func(metricsBucket) int64) int64 {
	var total int64
	for _, b := range v.recentBuckets(seconds) {
		total += field(b)
	}
	return total
}
