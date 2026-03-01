package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	lockdmcp "pkt.systems/lockd/mcp"
)

func main() {
	outPath := flag.String("out", "", "optional output file path (defaults to stdout)")
	flag.Parse()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	out, err := lockdmcp.BuildFullMCPSpecJSONL(ctx, lockdmcp.Config{
		DisableTLS:          true,
		UpstreamDisableMTLS: true,
		AllowHTTP:           true,
	})
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "getfullmcpspec: %v\n", err)
		os.Exit(1)
	}
	pretty, err := prettyJSONL(out)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "getfullmcpspec: pretty print: %v\n", err)
		os.Exit(1)
	}
	if err := writeOutput(*outPath, pretty); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "getfullmcpspec: write output: %v\n", err)
		os.Exit(1)
	}
}

func prettyJSONL(in []byte) ([]byte, error) {
	trimmed := bytes.TrimSpace(in)
	if len(trimmed) == 0 {
		return []byte{}, nil
	}
	lines := bytes.Split(trimmed, []byte("\n"))
	var out bytes.Buffer
	for i, line := range lines {
		line = bytes.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		var indented bytes.Buffer
		if err := json.Indent(&indented, line, "", "  "); err != nil {
			return nil, fmt.Errorf("line %d: %w", i+1, err)
		}
		out.Write(indented.Bytes())
		out.WriteByte('\n')
	}
	return out.Bytes(), nil
}

func writeOutput(outPath string, payload []byte) error {
	if outPath == "" {
		_, err := os.Stdout.Write(payload)
		return err
	}
	return os.WriteFile(outPath, payload, 0o644)
}
