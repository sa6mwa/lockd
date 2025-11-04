//go:build integration && aws

package awsintegration

import (
	"testing"

	"pkt.systems/lockd"
)

func resetAWSBucketForCrypto(tb testing.TB, cfg lockd.Config) {
	ResetAWSBucketForCrypto(tb, cfg)
}
