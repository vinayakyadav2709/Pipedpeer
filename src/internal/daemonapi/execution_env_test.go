package daemonapi

import "testing"

func TestGPULibraryPath(t *testing.T) {
	cases := []struct {
		name, driverDir, daemonPath, want string
	}{
		{"daemon has no path of its own", "/usr/lib64", "", "/usr/lib64"},
		{"daemon path is kept after the driver dir", "/usr/lib64", "/opt/cuda/lib", "/usr/lib64:/opt/cuda/lib"},
		{"daemon path with several entries", "/usr/lib64", "/opt/a:/opt/b", "/usr/lib64:/opt/a:/opt/b"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := gpuLibraryPath(tc.driverDir, tc.daemonPath); got != tc.want {
				t.Fatalf("gpuLibraryPath(%q, %q) = %q, want %q", tc.driverDir, tc.daemonPath, got, tc.want)
			}
		})
	}
}
