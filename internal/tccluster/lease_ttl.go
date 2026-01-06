package tccluster

import (
	"time"

	"pkt.systems/lockd/internal/tcleader"
)

const leaseTTLMultiplier = 3
const minLeaseTTL = time.Second

// DeriveLeaseTTL returns the membership lease TTL derived from the leader TTL.
func DeriveLeaseTTL(leaderTTL time.Duration) time.Duration {
	if leaderTTL <= 0 {
		leaderTTL = tcleader.DefaultLeaseTTL
	}
	ttl := leaderTTL * leaseTTLMultiplier
	if ttl < minLeaseTTL {
		return minLeaseTTL
	}
	return ttl
}
