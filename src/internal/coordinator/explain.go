package coordinator

import "github.com/pipedpeer/pipedpeer/internal/jobhistory"

// ExplainCandidates turns a placement decision into the record a run keeps of
// it: every node considered, the inputs its score was made of, and which one
// won.
func ExplainCandidates(d PlacementDecision) []jobhistory.Candidate {
	out := make([]jobhistory.Candidate, 0, len(d.Candidates))
	for _, c := range d.Candidates {
		n := c.Node
		host := n.Capabilities["hostname"]
		if host == "" {
			host = extractHost(n.SSHEndpoint)
		}
		out = append(out, jobhistory.Candidate{
			NodeID:        n.NodeID,
			Host:          host,
			Source:        string(c.Source),
			Score:         c.Score,
			Chosen:        !c.Rejected && n.NodeID != "" && n.NodeID == d.ChosenNode.NodeID,
			Rejected:      c.Rejected,
			Reason:        c.RejectReason,
			CPUPercent:    n.Load.CPUPercent,
			MemPercent:    n.Load.MemPercent,
			ActiveJobs:    n.Load.ActiveJobs,
			Cores:         totalCores(n),
			FreeCores:     freeCores(n),
			AvailMemBytes: n.Load.AvailableMemBytes,
			GPU:           n.Capabilities["gpu_name"],
			Health:        n.HealthScore,
		})
	}
	return out
}
