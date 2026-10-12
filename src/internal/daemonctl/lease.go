package daemonctl

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// ReleaseLease asks a node to free leaseID now (POST /v1/complete). A run
// cancelled before its job started has nothing on the node to end the lease,
// and a GPU node then refused every other run until the TTL ran out.
// Idempotent: the node ignores a lease it no longer holds.
func ReleaseLease(host string, port int, leaseID, status string) error {
	body, err := json.Marshal(map[string]string{"lease_id": leaseID, "status": status})
	if err != nil {
		return err
	}
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Post(fmt.Sprintf("http://%s:%d/v1/complete", host, port), "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("node answered %s", resp.Status)
	}
	return nil
}
