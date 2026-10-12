package app

import "testing"

// TestJobMemLimitGuessNeverBelowDefault pins the V1 rehearsal failure: the
// numpy import baseline (300 MiB) became a 600 MiB cap and the kernel killed
// 02_numpy_heavy.py, whose matrices alone need over 1 GB. Only --mem may set a
// ceiling below the no-estimate default.
func TestJobMemLimitGuessNeverBelowDefault(t *testing.T) {
	const mib = 1 << 20
	cases := []struct {
		est  int64
		tier string
		want int64
	}{
		{300 * mib, "import", noEstimateMemLimit},
		{754 * mib, "historical", noEstimateMemLimit},
		{3 << 30, "filesize", 6 << 30},
		{0, "", noEstimateMemLimit},
		{300 * mib, "user", 600 * mib},
		{100 * mib, "user", 512 * mib},
	}
	for _, c := range cases {
		if got := jobMemLimit(c.est, c.tier); got != c.want {
			t.Errorf("jobMemLimit(%d MiB, %q) = %d MiB, want %d MiB", c.est/mib, c.tier, got/mib, c.want/mib)
		}
	}
}
