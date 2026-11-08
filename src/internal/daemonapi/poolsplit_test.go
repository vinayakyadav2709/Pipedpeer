package daemonapi

import (
	"bytes"
	"encoding/json"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/schedule"
)

func TestPlanSharesSplitPolicy(t *testing.T) {
	shareMap := func(sh []schedule.Share) map[string]int {
		m := make(map[string]int)
		for _, s := range sh {
			m[s.Device.ID] = s.Items
		}
		return m
	}

	newPrimed := func() *poolManager {
		pm := newPoolManager()
		pm.rates.observe("fast", 10, time.Second)
		pm.rates.observe("slow", 10, 2*time.Second)
		return pm
	}

	cases := []struct {
		name         string
		env          string
		items        int
		peers        []string
		includeLocal bool
		// For headcount rows: exact expected map (absent key = not present).
		wantMap map[string]int
		// For "identical to unset" rows.
		wantMeasured bool
		// For warning rows: substring expected in log output.
		wantLog string
	}{
		{
			name:         "headcount even split",
			env:          "headcount",
			items:        20,
			peers:        []string{"fast", "slow"},
			includeLocal: false,
			wantMap:      map[string]int{"fast": 10, "slow": 10},
		},
		{
			name:         "headcount remainder goes to first",
			env:          "headcount",
			items:        21,
			peers:        []string{"fast", "slow"},
			includeLocal: false,
			wantMap:      map[string]int{"fast": 11, "slow": 10},
		},
		{
			name:         "headcount with local",
			env:          "headcount",
			items:        20,
			peers:        []string{"fast", "slow"},
			includeLocal: true,
			wantMap:      map[string]int{"local": 7, "fast": 7, "slow": 6},
		},
		{
			name:         "headcount single item",
			env:          "headcount",
			items:        1,
			peers:        []string{"fast", "slow"},
			includeLocal: false,
			wantMap:      map[string]int{"fast": 1},
		},
		{
			name:         "headcount zero items",
			env:          "headcount",
			items:        0,
			peers:        []string{"fast", "slow"},
			includeLocal: false,
			wantMap:      map[string]int{},
		},
		{
			name:         "headcount no peers with local",
			env:          "headcount",
			items:        20,
			peers:        []string{},
			includeLocal: true,
			wantMap:      map[string]int{"local": 20},
		},
		{
			name:         "headcount no peers no local no panic",
			env:          "headcount",
			items:        20,
			peers:        []string{},
			includeLocal: false,
			wantMap:      map[string]int{},
		},
		{
			name:         "unset uses measured plan",
			env:          "",
			items:        20,
			peers:        []string{"fast", "slow"},
			includeLocal: false,
			wantMeasured: true,
		},
		{
			name:         "measured uses measured plan",
			env:          "measured",
			items:        20,
			peers:        []string{"fast", "slow"},
			includeLocal: false,
			wantMeasured: true,
		},
		{
			name:         "unknown Headcount logs warning and uses measured",
			env:          "Headcount",
			items:        20,
			peers:        []string{"fast", "slow"},
			includeLocal: false,
			wantMeasured: true,
			wantLog:      `unknown PIPEDPEER_POOL_SPLIT="Headcount"; using measured`,
		},
		{
			name:         "typo headcout logs warning and uses measured",
			env:          "headcout",
			items:        20,
			peers:        []string{"fast", "slow"},
			includeLocal: false,
			wantMeasured: true,
			wantLog:      `unknown PIPEDPEER_POOL_SPLIT="headcout"; using measured`,
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv("PIPEDPEER_POOL_SPLIT", c.env)

			var buf bytes.Buffer
			if c.wantLog != "" {
				log.SetOutput(&buf)
				t.Cleanup(func() { log.SetOutput(os.Stderr) })
			}

			pm := newPrimed()

			got := pm.planShares(&chunk{items: make([]json.RawMessage, c.items)}, c.peers, c.includeLocal)

			if c.wantMeasured {
				// Compute the expected measured plan identically.
				pm2 := newPrimed()
				want := schedule.Compute(
					schedule.Options{Items: 20},
					pm2.devicesFor([]string{"fast", "slow"}, "", false),
				)
				if len(got) != len(want.Shares) {
					t.Fatalf("measured plan: got %d shares, want %d", len(got), len(want.Shares))
				}
				for i := range got {
					if got[i] != want.Shares[i] {
						t.Errorf("share[%d]: got %+v, want %+v", i, got[i], want.Shares[i])
					}
				}
			} else {
				gotMap := shareMap(got)
				if len(gotMap) != len(c.wantMap) {
					t.Fatalf("got shares %v, want %v", gotMap, c.wantMap)
				}
				for id, wantItems := range c.wantMap {
					if gotMap[id] != wantItems {
						t.Errorf("device %q: got %d items, want %d", id, gotMap[id], wantItems)
					}
				}
				for id := range gotMap {
					if _, ok := c.wantMap[id]; !ok {
						t.Errorf("unexpected device %q in result", id)
					}
				}
			}

			if c.wantLog != "" {
				if !strings.Contains(buf.String(), c.wantLog) {
					t.Errorf("log output %q does not contain %q", buf.String(), c.wantLog)
				}
			}
		})
	}
}
