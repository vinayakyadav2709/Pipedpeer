package daemonctl

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// RunningMaxConcurrent reads the max-concurrent cap from the daemon's /health
// on 127.0.0.1:port, so a restart can keep it. 0 (unlimited) when the daemon
// does not answer or predates the field; the failure is logged, not hidden.
func RunningMaxConcurrent(port int) int {
	return maxConcurrentAt(fmt.Sprintf("http://127.0.0.1:%d/health", port))
}

func maxConcurrentAt(url string) int {
	resp, err := (&http.Client{Timeout: 2 * time.Second}).Get(url)
	if err != nil {
		fmt.Printf("warn: could not read max-concurrent from %s, restarting with 0: %v\n", url, err)
		return 0
	}
	defer resp.Body.Close()
	var h struct {
		MaxConcurrent int `json:"max_concurrent"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&h); err != nil {
		fmt.Printf("warn: bad /health from %s, restarting with max-concurrent 0: %v\n", url, err)
		return 0
	}
	return h.MaxConcurrent
}
