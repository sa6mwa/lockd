//go:build !no_psi

package main

import (
	"context"

	"pkt.systems/psi"
)

func main() {
	psi.Run(func(ctx context.Context) int {
		return submain(ctx)
	})
}
