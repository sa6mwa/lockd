// Copyright 2018 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"bufio"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/magiconair/properties"
	ycsbclient "github.com/pingcap/go-ycsb/pkg/client"
	"github.com/pingcap/go-ycsb/pkg/measurement"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/spf13/cobra"
)

type warmupConfig struct {
	ops           int
	minWindows    int
	maxWindows    int
	stablePercent float64
	stableCount   int
	cooldown      time.Duration
}

func normalizeWarmupConfig(cfg warmupConfig) (warmupConfig, error) {
	if cfg.ops <= 0 {
		return cfg, nil
	}
	if cfg.minWindows <= 0 {
		cfg.minWindows = 1
	}
	if cfg.maxWindows <= 0 {
		cfg.maxWindows = cfg.minWindows
	}
	if cfg.maxWindows < cfg.minWindows {
		return cfg, fmt.Errorf("warmup max windows (%d) must be >= min windows (%d)", cfg.maxWindows, cfg.minWindows)
	}
	if cfg.stablePercent < 0 {
		return cfg, fmt.Errorf("warmup stable percent must be >= 0")
	}
	if cfg.stableCount <= 0 {
		cfg.stableCount = 2
	}
	return cfg, nil
}

func (cfg warmupConfig) enabled() bool {
	return cfg.ops > 0 && cfg.maxWindows > 0
}

func runWarmup(dbName string, baseProps *properties.Properties, cfg warmupConfig, doTransactions bool, command string) error {
	threadCount := baseProps.GetInt64(prop.ThreadCount, 1)
	if threadCount < 1 {
		threadCount = 1
	}
	if int64(cfg.ops) < threadCount {
		return fmt.Errorf("warmup ops (%d) must be >= threadcount (%d)", cfg.ops, threadCount)
	}

	samples := make([]float64, 0, cfg.maxWindows)
	for window := 1; window <= cfg.maxWindows; window++ {
		props := cloneProperties(baseProps)
		props.Set(prop.Command, command)
		if doTransactions {
			props.Set(prop.DoTransactions, "true")
		} else {
			props.Set(prop.DoTransactions, "false")
		}
		warmOps, err := applyWarmupOverrides(props, baseProps, cfg.ops, doTransactions)
		if err != nil {
			return err
		}
		measurement.InitMeasure(props)
		workload, db, err := buildContext(dbName, props)
		if err != nil {
			return err
		}
		start := time.Now()
		c := ycsbclient.NewClient(props, workload, db)
		c.Run(globalContext)
		elapsed := time.Since(start)
		workload.Close()
		db.Close()

		opsPerSec := float64(warmOps) / math.Max(elapsed.Seconds(), 0.000001)
		samples = append(samples, opsPerSec)
		stable := warmupStable(samples, cfg.stableCount, cfg.stablePercent)
		fmt.Printf("warmup window=%d/%d ops=%d elapsed=%s ops_per_sec=%.1f stable=%t\n",
			window,
			cfg.maxWindows,
			warmOps,
			elapsed,
			opsPerSec,
			stable)
		if window >= cfg.minWindows && stable {
			break
		}
	}

	if cfg.cooldown > 0 {
		fmt.Printf("warmup cooldown=%s\n", cfg.cooldown)
		time.Sleep(cfg.cooldown)
	}

	return nil
}

func applyWarmupOverrides(props *properties.Properties, baseProps *properties.Properties, ops int, doTransactions bool) (int64, error) {
	if doTransactions {
		props.Set(prop.OperationCount, strconv.Itoa(ops))
		props.Set(prop.WarmUpTime, "0")
		return int64(ops), nil
	}

	baseRecordCount := baseProps.GetInt64(prop.RecordCount, prop.RecordCountDefault)
	if baseRecordCount < 0 {
		baseRecordCount = 0
	}
	warmupRecordCount := baseRecordCount + int64(ops)
	props.Set(prop.RecordCount, strconv.FormatInt(warmupRecordCount, 10))
	props.Set(prop.InsertStart, strconv.FormatInt(baseRecordCount, 10))
	props.Set(prop.InsertCount, strconv.Itoa(ops))
	return int64(ops), nil
}

func warmupStable(samples []float64, stableCount int, stablePercent float64) bool {
	if stableCount <= 1 || stablePercent <= 0 {
		return false
	}
	if len(samples) < stableCount {
		return false
	}
	window := samples[len(samples)-stableCount:]
	minVal := window[0]
	maxVal := window[0]
	sum := 0.0
	for _, val := range window {
		if val < minVal {
			minVal = val
		}
		if val > maxVal {
			maxVal = val
		}
		sum += val
	}
	mean := sum / float64(len(window))
	if mean <= 0 {
		return false
	}
	allowance := mean * (stablePercent / 100.0)
	return (maxVal - minVal) <= allowance
}

func cloneProperties(src *properties.Properties) *properties.Properties {
	dst := properties.NewProperties()
	for key, value := range src.Map() {
		dst.Set(key, value)
	}
	return dst
}

func runClientCommandFunc(cmd *cobra.Command, args []string, doTransactions bool, command string) {
	db := args[0]
	perfLog := strings.TrimSpace(perfLogPath)
	perfEnabled := perfLog != "" && !strings.EqualFold(perfLog, "off")
	perfLogPath = perfLog
	perfOutputPath := ""
	perfOutputTemp := ""
	perfOutputTemporary := false
	if perfEnabled {
		tmp, err := os.CreateTemp("", "lockd-ycsb-measurement-*.txt")
		if err != nil {
			fmt.Fprintf(os.Stderr, "lockd-ycsb: failed to create temp output file: %v\n", err)
			perfEnabled = false
		} else {
			perfOutputTemp = tmp.Name()
			_ = tmp.Close()
		}
	}

	warmupCfg, err := normalizeWarmupConfig(warmupConfig{
		ops:           warmupOps,
		minWindows:    warmupMinWindows,
		maxWindows:    warmupMaxWindows,
		stablePercent: warmupStablePercent,
		stableCount:   warmupStableCount,
		cooldown:      warmupCooldown,
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "lockd-ycsb: warmup config invalid: %v\n", err)
		return
	}

	props := buildProperties(func(p *properties.Properties) {
		doTransFlag := "true"
		if !doTransactions {
			doTransFlag = "false"
		}
		p.Set(prop.DoTransactions, doTransFlag)
		p.Set(prop.Command, command)

		if cmd.Flags().Changed("threads") {
			p.Set(prop.ThreadCount, strconv.Itoa(threadsArg))
		}

		if cmd.Flags().Changed("target") {
			p.Set(prop.Target, strconv.Itoa(targetArg))
		}

		if cmd.Flags().Changed("interval") {
			p.Set(prop.LogInterval, strconv.Itoa(reportInterval))
		}

		if perfEnabled {
			existing := strings.TrimSpace(p.GetString(prop.MeasurementRawOutputFile, ""))
			if existing == "" && perfOutputTemp != "" {
				p.Set(prop.MeasurementRawOutputFile, perfOutputTemp)
				perfOutputPath = perfOutputTemp
				perfOutputTemporary = true
			} else {
				perfOutputPath = existing
			}
		}
	})
	if warmupCfg.enabled() {
		if err := runWarmup(db, props, warmupCfg, doTransactions, command); err != nil {
			fmt.Fprintf(os.Stderr, "lockd-ycsb: warmup failed: %v\n", err)
			return
		}
	}
	setupGlobal(db, props)

	fmt.Println("***************** properties *****************")
	for key, value := range globalProps.Map() {
		fmt.Printf("\"%s\"=\"%s\"\n", key, value)
	}
	fmt.Println("**********************************************")

	c := ycsbclient.NewClient(globalProps, globalWorkload, globalDB)
	start := time.Now()
	c.Run(globalContext)
	fmt.Println("**********************************************")
	runLine := fmt.Sprintf("Run finished, takes %s", time.Since(start))
	fmt.Println(runLine)
	measurement.Output()

	perfLines := []string{runLine}
	var outputLines []string
	if perfEnabled && perfOutputPath != "" {
		lines, err := readPerfLines(perfOutputPath)
		if err != nil {
			fmt.Fprintf(os.Stderr, "lockd-ycsb: failed to read measurement output: %v\n", err)
		} else {
			outputLines = lines
		}
		if perfOutputTemporary {
			printLines(outputLines)
			_ = os.Remove(perfOutputPath)
		}
	}
	for _, line := range outputLines {
		if isSummaryLine(line) {
			perfLines = append(perfLines, line)
		}
	}
	if perfEnabled && perfLogPath != "" {
		appendPerfLog(perfLogPath, db, command, perfLines)
	}
}

func runLoadCommandFunc(cmd *cobra.Command, args []string) {
	runClientCommandFunc(cmd, args, false, "load")
}

func runTransCommandFunc(cmd *cobra.Command, args []string) {
	runClientCommandFunc(cmd, args, true, "run")
}

var (
	threadsArg          int
	targetArg           int
	reportInterval      int
	perfLogPath         string
	warmupOps           int
	warmupMinWindows    int
	warmupMaxWindows    int
	warmupStablePercent float64
	warmupStableCount   int
	warmupCooldown      time.Duration
)

func initClientCommand(m *cobra.Command) {
	m.Flags().StringSliceVarP(&propertyFiles, "property_file", "P", nil, "Specify a property file")
	m.Flags().StringArrayVarP(&propertyValues, "prop", "p", nil, "Specify a property value with name=value")
	m.Flags().StringVar(&tableName, "table", "", "Use the table name instead of the default \""+prop.TableNameDefault+"\"")
	m.Flags().IntVar(&threadsArg, "threads", 1, "Execute using n threads (overrides threadcount property)")
	m.Flags().IntVar(&targetArg, "target", 0, "Attempt to do n operations per second (overrides target property)")
	m.Flags().IntVar(&reportInterval, "interval", 10, "Interval (seconds) for outputting measurements")
	m.Flags().StringVar(&perfLogPath, "perf-log", "performance.log", "Append summary metrics to this file (empty disables)")
	m.Flags().IntVar(&warmupOps, "warmup-ops", 0, "Warmup ops per window (0 disables)")
	m.Flags().IntVar(&warmupMinWindows, "warmup-min-windows", 2, "Minimum warmup windows before considering stability")
	m.Flags().IntVar(&warmupMaxWindows, "warmup-max-windows", 3, "Maximum warmup windows before running the benchmark")
	m.Flags().Float64Var(&warmupStablePercent, "warmup-stable-percent", 7.5, "Max percent spread across warmup windows to consider stable")
	m.Flags().IntVar(&warmupStableCount, "warmup-stable-count", 2, "Consecutive warmup windows used for stability check")
	m.Flags().DurationVar(&warmupCooldown, "warmup-cooldown", 10*time.Second, "Cooldown delay after warmup before benchmark")
}

func newLoadCommand() *cobra.Command {
	m := &cobra.Command{
		Use:   "load db",
		Short: "YCSB load benchmark",
		Args:  cobra.MinimumNArgs(1),
		Run:   runLoadCommandFunc,
	}
	initClientCommand(m)
	return m
}

func newRunCommand() *cobra.Command {
	m := &cobra.Command{
		Use:   "run db",
		Short: "YCSB run benchmark",
		Args:  cobra.MinimumNArgs(1),
		Run:   runTransCommandFunc,
	}
	initClientCommand(m)
	return m
}

func readPerfLines(path string) ([]string, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var lines []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		lines = append(lines, line)
	}
	if err := scanner.Err(); err != nil {
		return lines, err
	}
	return lines, nil
}

func isSummaryLine(line string) bool {
	return strings.Contains(line, " - Takes(s):")
}

func printLines(lines []string) {
	for _, line := range lines {
		fmt.Println(line)
	}
}

func appendPerfLog(path, dbName, phase string, lines []string) {
	if path == "" {
		return
	}
	if dir := filepath.Dir(path); dir != "" && dir != "." {
		_ = os.MkdirAll(dir, 0o755)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		fmt.Fprintf(os.Stderr, "lockd-ycsb: failed to open perf log: %v\n", err)
		return
	}
	defer f.Close()

	metadata := perfMetadata(dbName, phase)
	fmt.Fprintf(f, "## %s\n", time.Now().UTC().Format(time.RFC3339))
	fmt.Fprintln(f, metadata)
	for _, line := range lines {
		fmt.Fprintln(f, line)
	}
	fmt.Fprintln(f)
}

func perfMetadata(dbName, phase string) string {
	if globalProps == nil {
		return fmt.Sprintf("backend=%s phase=%s", dbName, phase)
	}
	workload := globalProps.GetString(prop.Workload, "core")
	recordcount := globalProps.GetString(prop.RecordCount, "")
	operationcount := globalProps.GetString(prop.OperationCount, "")
	threads := globalProps.GetString(prop.ThreadCount, "")
	target := globalProps.GetString(prop.Target, "")
	endpoints := perfEndpoints(dbName, globalProps)
	walFsyncInterval := perfWalFsyncInterval(dbName, globalProps)
	walCommitMaxOps := perfWalCommitMaxOps(dbName, globalProps)
	walCommitSingleWait := perfWalCommitSingleWait(dbName, globalProps)
	queryEngine := perfQueryEngine(dbName, globalProps)
	queryReturn := perfQueryReturn(dbName, globalProps)
	return fmt.Sprintf(
		"backend=%s phase=%s workload=%s recordcount=%s operationcount=%s threads=%s target=%s endpoints=%s wal_fsync_interval=%s wal_commit_max_ops=%s wal_commit_single_wait=%s query_engine=%s query_return=%s",
		dbName,
		phase,
		workload,
		recordcount,
		operationcount,
		threads,
		target,
		endpoints,
		walFsyncInterval,
		walCommitMaxOps,
		walCommitSingleWait,
		queryEngine,
		queryReturn,
	)
}

func perfEndpoints(dbName string, props *properties.Properties) string {
	if props == nil {
		return ""
	}
	switch strings.ToLower(dbName) {
	case "lockd":
		return strings.TrimSpace(props.GetString("lockd.endpoints", ""))
	case "etcd":
		return strings.TrimSpace(props.GetString("etcd.endpoints", ""))
	default:
		return ""
	}
}

func perfWalFsyncInterval(dbName string, props *properties.Properties) string {
	if !strings.EqualFold(dbName, "lockd") {
		return ""
	}
	if props != nil {
		if v := strings.TrimSpace(props.GetString("lockd.wal_fsync_interval", "")); v != "" {
			return v
		}
	}
	return strings.TrimSpace(os.Getenv("LOCKD_WAL_FSYNC_INTERVAL"))
}

func perfWalCommitMaxOps(dbName string, props *properties.Properties) string {
	if !strings.EqualFold(dbName, "lockd") {
		return ""
	}
	if props != nil {
		if v := strings.TrimSpace(props.GetString("lockd.wal_commit_max_ops", "")); v != "" {
			return v
		}
	}
	return strings.TrimSpace(os.Getenv("LOCKD_WAL_COMMIT_MAX_OPS"))
}

func perfWalCommitSingleWait(dbName string, props *properties.Properties) string {
	if !strings.EqualFold(dbName, "lockd") {
		return ""
	}
	if props != nil {
		if v := strings.TrimSpace(props.GetString("lockd.wal_commit_single_wait", "")); v != "" {
			return v
		}
	}
	return strings.TrimSpace(os.Getenv("LOCKD_WAL_COMMIT_SINGLE_WAIT"))
}

func perfQueryEngine(dbName string, props *properties.Properties) string {
	if !strings.EqualFold(dbName, "lockd") || props == nil {
		return ""
	}
	return strings.TrimSpace(strings.ToLower(props.GetString("lockd.query.engine", "")))
}

func perfQueryReturn(dbName string, props *properties.Properties) string {
	if !strings.EqualFold(dbName, "lockd") || props == nil {
		return ""
	}
	return strings.TrimSpace(strings.ToLower(props.GetString("lockd.query.return", "")))
}
