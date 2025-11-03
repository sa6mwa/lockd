package lsf

import (
	"bufio"
	"context"
	"errors"
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"pkt.systems/lockd/internal/loggingutil"
	"pkt.systems/pslog"

	"golang.org/x/sys/unix"
	"pkt.systems/lockd/internal/qrf"
)

// Config controls the LSF sampling cadence.
type Config struct {
	Enabled        bool
	SampleInterval time.Duration
	LogInterval    time.Duration
}

// Observer tracks high-level system metrics and forwards them to the QRF.
type Observer struct {
	cfg     Config
	qrf     *qrf.Controller
	logger  pslog.Logger
	running atomic.Bool

	queueProducerInflight atomic.Int64
	queueConsumerInflight atomic.Int64
	queueAckInflight      atomic.Int64
	lockInflight          atomic.Int64

	lastCPUTotal uint64
	lastCPUIdle  uint64
	lastLogTime  time.Time

	wg sync.WaitGroup

	loadBaseline1   float64
	loadBaseline5   float64
	loadBaseline15  float64
	loadBaselineSet bool
}

// NewObserver constructs an LSF observer.
func NewObserver(cfg Config, controller *qrf.Controller, logger pslog.Logger) *Observer {
	if cfg.SampleInterval <= 0 {
		cfg.SampleInterval = 200 * time.Millisecond
	}
	if cfg.LogInterval < 0 {
		cfg.LogInterval = 0
	}
	logger = loggingutil.EnsureLogger(logger)
	return &Observer{
		cfg:    cfg,
		qrf:    controller,
		logger: logger.With("sys", "control.lsf.observer"),
	}
}

// Start launches the sampling loop. Safe to call multiple times; only the first call starts the loop.
func (o *Observer) Start(ctx context.Context) {
	if !o.cfg.Enabled || o.qrf == nil {
		return
	}
	if !o.running.CompareAndSwap(false, true) {
		return
	}
	o.wg.Add(1)
	go func() {
		defer o.wg.Done()
		o.run(ctx)
	}()
}

// Wait blocks until the sampling loop has exited.
func (o *Observer) Wait() {
	o.wg.Wait()
}

// BeginQueueProducer records the start of a queue operation and returns a closure that must be invoked on completion.
func (o *Observer) BeginQueueProducer() func() {
	if !o.cfg.Enabled {
		return func() {}
	}
	o.queueProducerInflight.Add(1)
	return func() {
		o.queueProducerInflight.Add(-1)
	}
}

// BeginQueueConsumer records the start of a queue consume operation and returns a completion callback.
func (o *Observer) BeginQueueConsumer() func() {
	if !o.cfg.Enabled {
		return func() {}
	}
	o.queueConsumerInflight.Add(1)
	return func() {
		o.queueConsumerInflight.Add(-1)
	}
}

// BeginQueueAck records the start of an ack operation and returns a completion callback.
func (o *Observer) BeginQueueAck() func() {
	if !o.cfg.Enabled {
		return func() {}
	}
	o.queueAckInflight.Add(1)
	return func() {
		o.queueAckInflight.Add(-1)
	}
}

// BeginLockOp records the start of a lock API operation and returns a completion closure.
func (o *Observer) BeginLockOp() func() {
	if !o.cfg.Enabled {
		return func() {}
	}
	o.lockInflight.Add(1)
	return func() {
		o.lockInflight.Add(-1)
	}
}

func (o *Observer) run(ctx context.Context) {
	ticker := time.NewTicker(o.cfg.SampleInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case now := <-ticker.C:
			o.sample(now)
		}
	}
}

func (o *Observer) sample(ts time.Time) {
	if o.qrf == nil {
		return
	}
	var mem runtime.MemStats
	runtime.ReadMemStats(&mem)

	rss := mem.Sys
	if v, err := readRSSBytes(); err == nil && v > 0 {
		rss = v
	}
	memoryPercent := 0.0
	memoryIncludesReclaimable := false
	swapPercent := 0.0
	swapUsedBytes := uint64(0)
	load1 := 0.0
	load5 := 0.0
	load15 := 0.0
	if sys, err := gatherSystemUsage(); err == nil {
		memoryPercent = sys.memoryPercent
		memoryIncludesReclaimable = sys.memoryIncludesReclaimable
		swapPercent = sys.swapPercent
		swapUsedBytes = sys.swapBytes
		load1 = sys.load1
		load5 = sys.load5
		load15 = sys.load15
	}
	cpuPercent := o.systemCPUPercent()

	loadBase1, loadBase5, loadBase15, loadMult1, loadMult5, loadMult15 := o.updateLoadBaselines(load1, load5, load15)

	snapshot := qrf.Snapshot{
		QueueProducerInflight:           o.queueProducerInflight.Load(),
		QueueConsumerInflight:           o.queueConsumerInflight.Load(),
		QueueAckInflight:                o.queueAckInflight.Load(),
		LockInflight:                    o.lockInflight.Load(),
		RSSBytes:                        rss,
		SwapBytes:                       swapUsedBytes,
		SystemMemoryUsedPercent:         memoryPercent,
		SystemMemoryIncludesReclaimable: memoryIncludesReclaimable,
		SystemSwapUsedPercent:           swapPercent,
		SystemCPUPercent:                cpuPercent,
		SystemLoad1:                     load1,
		SystemLoad5:                     load5,
		SystemLoad15:                    load15,
		Load1Baseline:                   loadBase1,
		Load5Baseline:                   loadBase5,
		Load15Baseline:                  loadBase15,
		Load1Multiplier:                 loadMult1,
		Load5Multiplier:                 loadMult5,
		Load15Multiplier:                loadMult15,
		Goroutines:                      runtime.NumGoroutine(),
		CollectedAt:                     ts,
	}
	if o.logger != nil && o.cfg.LogInterval > 0 && (o.lastLogTime.IsZero() || ts.Sub(o.lastLogTime) >= o.cfg.LogInterval) {
		o.logger.Debug("lockd.lsf.sample",
			"queue_producer_inflight", snapshot.QueueProducerInflight,
			"queue_consumer_inflight", snapshot.QueueConsumerInflight,
			"queue_ack_inflight", snapshot.QueueAckInflight,
			"lock_inflight", snapshot.LockInflight,
			"rss_bytes", snapshot.RSSBytes,
			"swap_bytes", snapshot.SwapBytes,
			"system_memory_percent", snapshot.SystemMemoryUsedPercent,
			"system_memory_includes_reclaimable", snapshot.SystemMemoryIncludesReclaimable,
			"system_swap_percent", snapshot.SystemSwapUsedPercent,
			"system_cpu_percent", snapshot.SystemCPUPercent,
			"system_load1", snapshot.SystemLoad1,
			"system_load5", snapshot.SystemLoad5,
			"system_load15", snapshot.SystemLoad15,
			"load1_baseline", snapshot.Load1Baseline,
			"load5_baseline", snapshot.Load5Baseline,
			"load15_baseline", snapshot.Load15Baseline,
			"load1_multiplier", snapshot.Load1Multiplier,
			"load5_multiplier", snapshot.Load5Multiplier,
			"load15_multiplier", snapshot.Load15Multiplier,
			"goroutines", snapshot.Goroutines,
		)
		o.lastLogTime = ts
	}
	o.qrf.Observe(snapshot)
}

func (o *Observer) updateLoadBaselines(load1, load5, load15 float64) (float64, float64, float64, float64, float64, float64) {
	const alpha = 0.05
	if !o.loadBaselineSet {
		o.loadBaseline1 = initialBaseline(load1)
		o.loadBaseline5 = initialBaseline(load5)
		o.loadBaseline15 = initialBaseline(load15)
		o.loadBaselineSet = true
	}
	o.loadBaseline1 = ewma(o.loadBaseline1, load1, alpha)
	o.loadBaseline5 = ewma(o.loadBaseline5, load5, alpha)
	o.loadBaseline15 = ewma(o.loadBaseline15, load15, alpha)

	return o.loadBaseline1, o.loadBaseline5, o.loadBaseline15,
		ratio(load1, o.loadBaseline1),
		ratio(load5, o.loadBaseline5),
		ratio(load15, o.loadBaseline15)
}

func initialBaseline(load float64) float64 {
	if load <= 0 {
		return 0.1
	}
	return load
}

func ewma(current, value, alpha float64) float64 {
	if current <= 0 {
		return value
	}
	return current + (value-current)*alpha
}

func ratio(value, baseline float64) float64 {
	if baseline <= 0 {
		return 0
	}
	return value / baseline
}

func (o *Observer) systemCPUPercent() float64 {
	total, idle, err := readSystemCPUStat()
	if err != nil {
		return 0
	}
	if o.lastCPUTotal == 0 && o.lastCPUIdle == 0 {
		o.lastCPUTotal = total
		o.lastCPUIdle = idle
		return 0
	}
	deltaTotal := total - o.lastCPUTotal
	deltaIdle := idle - o.lastCPUIdle
	o.lastCPUTotal = total
	o.lastCPUIdle = idle
	if deltaTotal == 0 || deltaTotal < deltaIdle {
		return 0
	}
	return (float64(deltaTotal-deltaIdle) / float64(deltaTotal)) * 100
}

type systemUsage struct {
	memoryPercent             float64
	memoryIncludesReclaimable bool
	swapPercent               float64
	swapBytes                 uint64
	load1                     float64
	load5                     float64
	load15                    float64
}

type meminfo struct {
	totalBytes              uint64
	availableBytes          uint64
	includesReclaimableData bool
}

func readRSSBytes() (uint64, error) {
	f, err := os.Open("/proc/self/statm")
	if err != nil {
		return 0, err
	}
	defer f.Close()
	reader := bufio.NewReader(f)
	line, err := reader.ReadString('\n')
	if err != nil && err != io.EOF {
		return 0, err
	}
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return 0, errors.New("unexpected statm contents")
	}
	pages, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0, err
	}
	return pages * uint64(os.Getpagesize()), nil
}

func readMeminfo() (meminfo, error) {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return meminfo{}, err
	}
	defer f.Close()
	return parseMeminfo(f)
}

func parseMeminfo(r io.Reader) (meminfo, error) {
	scanner := bufio.NewScanner(r)
	fields := make(map[string]uint64)
	for scanner.Scan() {
		text := scanner.Text()
		if text == "" {
			continue
		}
		parts := strings.Fields(text)
		if len(parts) < 2 {
			continue
		}
		name := strings.TrimSuffix(parts[0], ":")
		value, err := strconv.ParseUint(parts[1], 10, 64)
		if err != nil {
			continue
		}
		fields[name] = value
	}
	if err := scanner.Err(); err != nil {
		return meminfo{}, err
	}
	totalKB, ok := fields["MemTotal"]
	if !ok || totalKB == 0 {
		return meminfo{}, errors.New("meminfo missing MemTotal")
	}
	totalBytes := totalKB * 1024
	if availKB, ok := fields["MemAvailable"]; ok && availKB > 0 {
		availableBytes := min(availKB * 1024, totalBytes)
		return meminfo{
			totalBytes:              totalBytes,
			availableBytes:          availableBytes,
			includesReclaimableData: true,
		}, nil
	}

	memFreeKB := fields["MemFree"]
	buffersKB := fields["Buffers"]
	cachedKB := fields["Cached"]
	sreclaimableKB := fields["SReclaimable"]
	shmemKB := fields["Shmem"]

	availableKB := int64(memFreeKB)
	availableKB += int64(buffersKB)
	availableKB += int64(cachedKB)
	availableKB += int64(sreclaimableKB)
	availableKB -= int64(shmemKB)
	if availableKB < 0 {
		availableKB = 0
	}
	availableBytes := min(uint64(availableKB) * 1024, totalBytes)

	includesReclaimable := buffersKB > 0 || cachedKB > 0 || sreclaimableKB > 0

	return meminfo{
		totalBytes:              totalBytes,
		availableBytes:          availableBytes,
		includesReclaimableData: includesReclaimable,
	}, nil
}

func gatherSystemUsage() (systemUsage, error) {
	var si unix.Sysinfo_t
	if err := unix.Sysinfo(&si); err != nil {
		return systemUsage{}, err
	}
	memoryIncludesReclaimable := false
	unit := uint64(si.Unit)
	if unit == 0 {
		unit = 1
	}
	totalRAM := uint64(si.Totalram) * unit
	if totalRAM == 0 {
		return systemUsage{}, errors.New("sysinfo: totalram reported as zero")
	}
	freeRAM := uint64(si.Freeram) * unit
	bufferRAM := uint64(si.Bufferram) * unit
	available := min(freeRAM + bufferRAM, totalRAM)
	if mi, err := readMeminfo(); err == nil && mi.totalBytes > 0 && mi.availableBytes > 0 {
		totalRAM = mi.totalBytes
		available = min(mi.availableBytes, totalRAM)
		memoryIncludesReclaimable = mi.includesReclaimableData
	}
	memoryUsed := 1 - float64(available)/float64(totalRAM)
	if memoryUsed < 0 {
		memoryUsed = 0
	}
	if memoryUsed > 1 {
		memoryUsed = 1
	}

	totalSwap := uint64(si.Totalswap) * unit
	freeSwap := uint64(si.Freeswap) * unit
	swapUsed := uint64(0)
	swapPercent := 0.0
	if totalSwap > 0 {
		if freeSwap > totalSwap {
			freeSwap = totalSwap
		}
		swapUsed = totalSwap - freeSwap
		swapPercent = float64(swapUsed) / float64(totalSwap) * 100
	}

	const loadScale = 65536.0
	load1 := float64(si.Loads[0]) / loadScale
	load5 := float64(si.Loads[1]) / loadScale
	load15 := float64(si.Loads[2]) / loadScale

	return systemUsage{
		memoryPercent:             memoryUsed * 100,
		memoryIncludesReclaimable: memoryIncludesReclaimable,
		swapPercent:               swapPercent,
		swapBytes:                 swapUsed,
		load1:                     load1,
		load5:                     load5,
		load15:                    load15,
	}, nil
}

func readSystemCPUStat() (uint64, uint64, error) {
	f, err := os.Open("/proc/stat")
	if err != nil {
		return 0, 0, err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	if scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 5 || fields[0] != "cpu" {
			return 0, 0, errors.New("unexpected /proc/stat format")
		}
		var values []uint64
		for _, field := range fields[1:] {
			val, err := strconv.ParseUint(field, 10, 64)
			if err != nil {
				return 0, 0, err
			}
			values = append(values, val)
		}
		var idle uint64
		if len(values) >= 4 {
			idle = values[3]
		}
		total := uint64(0)
		for _, v := range values {
			total += v
		}
		return total, idle, nil
	}
	if err := scanner.Err(); err != nil {
		return 0, 0, err
	}
	return 0, 0, errors.New("no cpu line in /proc/stat")
}
