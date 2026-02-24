package main

import (
	"context"
	"fmt"
	"os"
	"time"

	lockdmcp "pkt.systems/lockd/mcp"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	out, err := lockdmcp.BuildToolsListResponseJSON(ctx, lockdmcp.Config{
		DisableTLS:          true,
		UpstreamDisableMTLS: true,
		AllowHTTP:           true,
	})
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "getmcptoolslist: %v\n", err)
		os.Exit(1)
	}
	_, _ = os.Stdout.Write(out)
}
