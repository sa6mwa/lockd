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
	"context"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/chzyer/readline"
	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/util"

	"github.com/spf13/cobra"
)

func newShellCommand() *cobra.Command {
	m := &cobra.Command{
		Use:   "shell db",
		Short: "YCSB Command Line Client",
		Args:  cobra.MinimumNArgs(1),
		Run:   runShellCommandFunc,
	}
	m.Flags().StringSliceVarP(&propertyFiles, "property_file", "P", nil, "Specify a property file")
	m.Flags().StringSliceVarP(&propertyValues, "prop", "p", nil, "Specify a property value with name=value")
	m.Flags().StringVar(&tableName, "table", "", "Use the table name instead of the default \""+prop.TableNameDefault+"\"")
	return m
}

var shellContext context.Context

func runShellCommandFunc(cmd *cobra.Command, args []string) {
	db := args[0]
	initialGlobal(db, nil)

	shellContext = globalWorkload.InitThread(globalContext, 0, 1)
	shellContext = globalDB.InitThread(shellContext, 0, 1)

	shellLoop()

	globalDB.CleanupThread(shellContext)
	globalWorkload.CleanupThread(shellContext)
}

func shellLoop() {
	rl, err := readline.NewEx(&readline.Config{
		Prompt:       "ycsb> ",
		AutoComplete: completer,
	})
	if err != nil {
		panic(err)
	}
	defer rl.Close()

	for {
		line, err := rl.Readline()
		if err == readline.ErrInterrupt {
			if len(line) == 0 {
				break
			}
			continue
		} else if err == io.EOF {
			break
		}

		line = strings.TrimSpace(line)
		if len(line) == 0 {
			continue
		}

		switch line {
		case "exit", "quit":
			return
		}

		runShellCommand(strings.Split(line, " "))
	}
}

func runShellCommand(args []string) {
	cmd := &cobra.Command{
		Use:   "shell",
		Short: "YCSB shell command",
	}

	cmd.SetArgs(args)
	cmd.ParseFlags(args)

	cmd.AddCommand(
		&cobra.Command{
			Use:                   "read key [field0 field1 field2 ...]",
			Short:                 "Read a record",
			Args:                  cobra.MinimumNArgs(1),
			Run:                   runShellReadCommand,
			DisableFlagsInUseLine: true,
		},
		&cobra.Command{
			Use:                   "scan key recordcount [field0 field1 field2 ...]",
			Short:                 "Scan starting at key",
			Args:                  cobra.MinimumNArgs(2),
			Run:                   runShellScanCommand,
			DisableFlagsInUseLine: true,
		},
		&cobra.Command{
			Use:                   "update key values",
			Short:                 "Update a record",
			Args:                  cobra.MinimumNArgs(2),
			Run:                   runShellUpdateCommand,
			DisableFlagsInUseLine: true,
		},
		&cobra.Command{
			Use:                   "insert key values",
			Short:                 "Insert a record",
			Args:                  cobra.MinimumNArgs(2),
			Run:                   runShellInsertCommand,
			DisableFlagsInUseLine: true,
		},
		&cobra.Command{
			Use:                   "delete key",
			Short:                 "Delete a record",
			Args:                  cobra.MinimumNArgs(1),
			Run:                   runShellDeleteCommand,
			DisableFlagsInUseLine: true,
		},
	)

	cmd.Execute()
}

var completer = readline.NewPrefixCompleter(
	readline.PcItem("read"),
	readline.PcItem("scan"),
	readline.PcItem("update"),
	readline.PcItem("insert"),
	readline.PcItem("delete"),
)

func runShellReadCommand(cmd *cobra.Command, args []string) {
	key := args[0]
	fields := args[1:]

	values, err := globalDB.Read(shellContext, tableName, key, fields)
	if err != nil {
		util.Fatalf("read: %v", err)
	}

	fmt.Printf("READ %s => %v\n", key, values)
}

func runShellScanCommand(cmd *cobra.Command, args []string) {
	key := args[0]
	count, err := strconv.Atoi(args[1])
	if err != nil {
		util.Fatalf("scan parse count: %v", err)
	}
	fields := args[2:]

	result, err := globalDB.Scan(shellContext, tableName, key, count, fields)
	if err != nil {
		util.Fatalf("scan: %v", err)
	}

	fmt.Printf("SCAN %s %d => %v\n", key, count, result)
}

func runShellUpdateCommand(cmd *cobra.Command, args []string) {
	key := args[0]
	params := parseValues(args[1:])

	err := globalDB.Update(shellContext, tableName, key, params)
	if err != nil {
		util.Fatalf("update: %v", err)
	}

	fmt.Printf("UPDATE %s => %v\n", key, params)
}

func runShellInsertCommand(cmd *cobra.Command, args []string) {
	key := args[0]
	params := parseValues(args[1:])

	err := globalDB.Insert(shellContext, tableName, key, params)
	if err != nil {
		util.Fatalf("insert: %v", err)
	}

	fmt.Printf("INSERT %s => %v\n", key, params)
}

func runShellDeleteCommand(cmd *cobra.Command, args []string) {
	key := args[0]

	err := globalDB.Delete(shellContext, tableName, key)
	if err != nil {
		util.Fatalf("delete: %v", err)
	}

	fmt.Printf("DELETE %s\n", key)
}

func parseValues(args []string) map[string][]byte {
	res := make(map[string][]byte, len(args))
	for _, arg := range args {
		kv := strings.Split(arg, "=")
		if len(kv) != 2 {
			util.Fatalf("invalid arg %s", arg)
		}
		res[kv[0]] = []byte(kv[1])
	}
	return res
}
