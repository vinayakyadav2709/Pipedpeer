package daemonapi

import (
	"log"
	"os"

	"github.com/pipedpeer/pipedpeer/internal/schedule"
)

// splitEnv names the split policy; "" and "measured" keep the scheduler's plan.
const splitEnv = "PIPEDPEER_POOL_SPLIT"

// headcountShares gives every device the same number of items, the remainder
// one each to the first devices in order; devices with 0 items are left out.
// Used only when PIPEDPEER_POOL_SPLIT=headcount (experiment E4).
func headcountShares(items int, devices []schedule.Device) []schedule.Share {
	if len(devices) == 0 {
		return nil
	}
	if items == 0 {
		return nil
	}
	base := items / len(devices)
	remainder := items % len(devices)
	var shares []schedule.Share
	for i, d := range devices {
		n := base
		if i < remainder {
			n++
		}
		if n == 0 {
			continue
		}
		shares = append(shares, schedule.Share{Device: d, Items: n})
	}
	return shares
}

// splitPolicy returns "measured" or "headcount"; any other value logs
// `[pool] unknown PIPEDPEER_POOL_SPLIT=%q; using measured` once per call and
// returns "measured".
func splitPolicy() string {
	v := os.Getenv(splitEnv)
	switch v {
	case "", "measured":
		return "measured"
	case "headcount":
		return "headcount"
	default:
		log.Printf("[pool] unknown PIPEDPEER_POOL_SPLIT=%q; using measured", v)
		return "measured"
	}
}
