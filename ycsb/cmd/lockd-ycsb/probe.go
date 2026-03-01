package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/pingcap/go-ycsb/pkg/prop"
	"github.com/pingcap/go-ycsb/pkg/ycsb"
	"github.com/spf13/cobra"
)

var probeTimeout time.Duration

func newProbeCommand() *cobra.Command {
	m := &cobra.Command{
		Use:   "probe db",
		Short: "Probe DB connectivity without running a benchmark",
		Args:  cobra.MinimumNArgs(1),
		RunE:  runProbeCommandFunc,
	}
	m.Flags().StringSliceVarP(&propertyFiles, "property_file", "P", nil, "Specify a property file")
	m.Flags().StringArrayVarP(&propertyValues, "prop", "p", nil, "Specify a property value with name=value")
	m.Flags().StringVar(&tableName, "table", "", "Use the table name instead of the default \""+prop.TableNameDefault+"\"")
	m.Flags().DurationVar(&probeTimeout, "probe-timeout", 3*time.Second, "Probe timeout")
	return m
}

func runProbeCommandFunc(_ *cobra.Command, args []string) error {
	dbName := args[0]
	props := buildProperties(nil)
	if probeTimeout <= 0 {
		probeTimeout = 3 * time.Second
	}
	table := tableName
	if table == "" {
		table = props.GetString(prop.TableName, prop.TableNameDefault)
	}

	dbCreator := ycsb.GetDBCreator(dbName)
	if dbCreator == nil {
		return fmt.Errorf("%s is not registered", dbName)
	}
	db, err := dbCreator.Create(props)
	if err != nil {
		return err
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), probeTimeout)
	defer cancel()
	_, err = db.Read(ctx, table, "__ycsb_probe__", nil)
	if err == nil || isProbeExpectedReadError(err) {
		fmt.Printf("probe ok: %s\n", dbName)
		return nil
	}
	return fmt.Errorf("probe failed: %w", err)
}

func isProbeExpectedReadError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "not found") || strings.Contains(message, "not exist")
}
