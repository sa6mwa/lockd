//go:build no_psi

package main

import (
	"context"
	"os"
)

func main() {
	os.Exit(submain(context.Background()))
}
