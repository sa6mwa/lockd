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

	initialGlobal(db, func() {
		doTransFlag := "true"
		if !doTransactions {
			doTransFlag = "false"
		}
		globalProps.Set(prop.DoTransactions, doTransFlag)
		globalProps.Set(prop.Command, command)

		if cmd.Flags().Changed("threads") {
			globalProps.Set(prop.ThreadCount, strconv.Itoa(threadsArg))
		}

		if cmd.Flags().Changed("target") {
			globalProps.Set(prop.Target, strconv.Itoa(targetArg))
		}

		if cmd.Flags().Changed("interval") {
			globalProps.Set(prop.LogInterval, strconv.Itoa(reportInterval))
		}

		if perfEnabled {
			existing := strings.TrimSpace(globalProps.GetString(prop.MeasurementRawOutputFile, ""))
			if existing == "" && perfOutputTemp != "" {
				globalProps.Set(prop.MeasurementRawOutputFile, perfOutputTemp)
				perfOutputPath = perfOutputTemp
				perfOutputTemporary = true
			} else {
				perfOutputPath = existing
			}
		}
	})

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
	threadsArg     int
	targetArg      int
	reportInterval int
	perfLogPath    string
)

func initClientCommand(m *cobra.Command) {
	m.Flags().StringSliceVarP(&propertyFiles, "property_file", "P", nil, "Specify a property file")
	m.Flags().StringArrayVarP(&propertyValues, "prop", "p", nil, "Specify a property value with name=value")
	m.Flags().StringVar(&tableName, "table", "", "Use the table name instead of the default \""+prop.TableNameDefault+"\"")
	m.Flags().IntVar(&threadsArg, "threads", 1, "Execute using n threads (overrides threadcount property)")
	m.Flags().IntVar(&targetArg, "target", 0, "Attempt to do n operations per second (overrides target property)")
	m.Flags().IntVar(&reportInterval, "interval", 10, "Interval (seconds) for outputting measurements")
	m.Flags().StringVar(&perfLogPath, "perf-log", "performance.log", "Append summary metrics to this file (empty disables)")
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
	return fmt.Sprintf(
		"backend=%s phase=%s workload=%s recordcount=%s operationcount=%s threads=%s target=%s endpoints=%s",
		dbName,
		phase,
		workload,
		recordcount,
		operationcount,
		threads,
		target,
		endpoints,
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
