package daemonapi

import (
	"encoding/json"
	"fmt"
	"net/http"
)

type Server struct {
	nodeID string
	mux    *http.ServeMux
}

type acceptRequest struct {
	TargetID string `json:"target_id"`
	JobName  string `json:"job_name"`
}

type acceptResponse struct {
	Accepted bool   `json:"accepted"`
	NodeID   string `json:"node_id"`
	Reason   string `json:"reason,omitempty"`
}

func New(nodeID string) *Server {
	s := &Server{nodeID: nodeID, mux: http.NewServeMux()}
	s.routes()
	return s
}

func (s *Server) Handler() http.Handler {
	return s.mux
}

func (s *Server) ListenAndServe(port int) error {
	addr := fmt.Sprintf(":%d", port)
	return http.ListenAndServe(addr, s.Handler())
}

func (s *Server) routes() {
	s.mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{
			"status":  "ok",
			"node_id": s.nodeID,
		})
	})

	s.mux.HandleFunc("/v1/accept", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			writeJSON(w, http.StatusMethodNotAllowed, acceptResponse{Accepted: false, NodeID: s.nodeID, Reason: "method not allowed"})
			return
		}

		var req acceptRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, acceptResponse{Accepted: false, NodeID: s.nodeID, Reason: "invalid request body"})
			return
		}
		if req.TargetID == "" {
			writeJSON(w, http.StatusBadRequest, acceptResponse{Accepted: false, NodeID: s.nodeID, Reason: "target_id is required"})
			return
		}
		if req.TargetID != s.nodeID {
			writeJSON(w, http.StatusConflict, acceptResponse{Accepted: false, NodeID: s.nodeID, Reason: "target_id does not match this node"})
			return
		}

		writeJSON(w, http.StatusOK, acceptResponse{Accepted: true, NodeID: s.nodeID})
	})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
