package daemonapi

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/rs/zerolog/log"
	"github.com/shirou/gopsutil/v4/process"
)

// jobOutputFile is the daemon's own record of everything a job sent its
// client, one storedOutput per line, so `attach` replays exactly what a live
// client saw.
const jobOutputFile = "output.jsonl"

// attachBuffer is how many frames a live attacher may fall behind before it
// is cut loose to catch up from the file.
const attachBuffer = 256

// jobOutputCap bounds output.jsonl per job, so a chatty background job can't
// fill the disk. Past it the file keeps its newer half, behind a note frame.
var jobOutputCap int64 = 50 << 20

// storedOutput is one line of output.jsonl. Seq lets an attacher that read
// the file and then follows live frames skip what it already sent; it never
// goes on the wire.
type storedOutput struct {
	Seq int `json:"seq"`
	OutputMessage
}

// jobOutputLog appends a job's output frames to <jobDir>/output.jsonl and fans
// them out to live attachers.
type jobOutputLog struct {
	mu       sync.Mutex
	jobID    string
	path     string
	f        *os.File
	seq      int
	size     int64
	done     bool
	final    *OutputMessage
	subs     map[chan storedOutput]struct{}
	warned   bool
	cancelFn func()
}

// jobLogs holds the logs of jobs still writing, keyed by file path (unique per
// daemon, since every Server has its own jobDir). A log leaves it when its job
// finishes (or its dir is pruned); a finished job's log is read from disk.
var jobLogs sync.Map

// jobOutput returns the output log of jobID, opening (or creating) its file.
// A log reopened after a daemon restart continues the file's sequence.
func (s *Server) jobOutput(jobID string) *jobOutputLog {
	path := filepath.Join(s.jobDir, jobID, jobOutputFile)
	if l, ok := jobLogs.Load(path); ok {
		return l.(*jobOutputLog)
	}
	l := &jobOutputLog{jobID: jobID, path: path, subs: map[chan storedOutput]struct{}{}}
	if info, err := os.Stat(path); err == nil {
		l.size = info.Size()
	}
	for _, m := range readStoredOutput(path, 0) {
		l.seq = m.Seq
		if m.Done {
			l.done = true
			final := m.OutputMessage
			l.final = &final
		}
	}
	if l.done {
		// Finished: nothing will write to it, so it needs no shared entry.
		return l
	}
	actual, _ := jobLogs.LoadOrStore(path, l)
	return actual.(*jobOutputLog)
}

// Append records m and hands it to every live attacher. It never blocks the
// job: an attacher whose buffer is full is cut loose (its channel closed) and
// catches up from the file.
func (l *jobOutputLog) Append(m OutputMessage) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.done {
		log.Warn().Str("job", l.jobID).Msg("job output after the done frame; not recorded")
		return
	}
	l.write(m)
}

// Finish records the done frame, closes the file and ends every live
// attacher's stream (they read the rest from the file).
func (l *jobOutputLog) Finish(final OutputMessage) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.done {
		log.Warn().Str("job", l.jobID).Msg("job output finished twice; second done frame not recorded")
		return
	}
	final.Done = true
	l.write(final)
	l.done = true
	l.final = &final
	if l.f != nil {
		if err := l.f.Close(); err != nil {
			log.Warn().Err(err).Str("file", l.path).Msg("closing job output log")
		}
		l.f = nil
	}
	for ch := range l.subs {
		close(ch)
		delete(l.subs, ch)
	}
	// Every attacher was just ended, so drop the entry: an attach from now on
	// replays the file. Unless a write failed — then this log is the only
	// record of the done frame and stays.
	if !l.warned {
		jobLogs.CompareAndDelete(l.path, l)
	}
}

// SetCancel registers how to stop the job, so a cancel frame sent on an
// attach connection stops it like one sent on the exec connection.
func (l *jobOutputLog) SetCancel(fn func()) {
	l.mu.Lock()
	l.cancelFn = fn
	l.mu.Unlock()
}

func (l *jobOutputLog) cancel() bool {
	l.mu.Lock()
	fn := l.cancelFn
	l.mu.Unlock()
	if fn == nil {
		return false
	}
	fn()
	return true
}

// write appends one frame; l.mu is held.
func (l *jobOutputLog) write(m OutputMessage) {
	l.seq++
	rec := storedOutput{Seq: l.seq, OutputMessage: m}
	if l.f == nil {
		f, err := os.OpenFile(l.path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			l.warnOnce(err, "opening job output log; attach will miss this output")
		} else {
			l.f = f
		}
	}
	if l.f != nil {
		data, _ := json.Marshal(rec)
		if n, err := l.f.Write(append(data, '\n')); err != nil {
			l.warnOnce(err, "writing job output log; attach will miss this output")
		} else if l.size += int64(n); l.size > jobOutputCap {
			l.trim()
		}
	}
	for ch := range l.subs {
		select {
		case ch <- rec:
		default:
			log.Warn().Str("job", l.jobID).Msg("attacher fell behind; it catches up from the output log")
			close(ch)
			delete(l.subs, ch)
		}
	}
}

func (l *jobOutputLog) warnOnce(err error, msg string) {
	if l.warned {
		return
	}
	l.warned = true
	log.Warn().Err(err).Str("job", l.jobID).Str("file", l.path).Msg(msg)
}

// follow returns the recorded frames after seq and, unless the log is
// finished, a channel of the frames that follow (closed at Finish or when
// the attacher falls behind). Both are taken under one lock, so nothing is
// missed between them. ch is nil when the log is finished.
// ponytail: reads the whole file on each (re)subscribe; fine for job output
// sizes, seek by offset if logs grow to hundreds of MB.
func (l *jobOutputLog) follow(after int) (past []storedOutput, ch chan storedOutput) {
	l.mu.Lock()
	defer l.mu.Unlock()
	past = readStoredOutput(l.path, after)
	if l.done {
		return past, nil
	}
	ch = make(chan storedOutput, attachBuffer)
	l.subs[ch] = struct{}{}
	return past, ch
}

func (l *jobOutputLog) unsubscribe(ch chan storedOutput) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.subs[ch]; ok {
		close(ch)
		delete(l.subs, ch)
	}
}

// finalFrame is the recorded done frame, or nil if the job has not finished.
func (l *jobOutputLog) finalFrame() *OutputMessage {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.final
}

// readStoredOutput returns the frames in path with Seq > after. A missing file
// is an empty log; a line that does not parse is skipped with a warning.
func readStoredOutput(path string, after int) []storedOutput {
	f, err := os.Open(path)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Warn().Err(err).Str("file", path).Msg("reading job output log")
		}
		return nil
	}
	defer f.Close()
	var out []storedOutput
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 64*1024), 64*1024*1024)
	for sc.Scan() {
		var m storedOutput
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			log.Warn().Err(err).Str("file", path).Msg("skipping unreadable job output line")
			continue
		}
		if m.Seq > after {
			out = append(out, m)
		}
	}
	if err := sc.Err(); err != nil {
		log.Warn().Err(err).Str("file", path).Msg("reading job output log")
	}
	return out
}

// trim rewrites the file to its newest frames (at most half the cap) behind a
// note saying how much was dropped; l.mu is held. Halving keeps the rewrite
// cost linear in the output. The note takes the seq of the last dropped frame,
// so an attacher that already sent that frame skips the note too.
func (l *jobOutputLog) trim() {
	recs := readStoredOutput(l.path, 0)
	var kept int64
	cut := len(recs)
	for cut > 0 {
		data, _ := json.Marshal(recs[cut-1])
		if kept+int64(len(data))+1 > jobOutputCap/2 {
			break
		}
		kept += int64(len(data)) + 1
		cut--
	}
	if cut == 0 {
		return
	}
	note := storedOutput{Seq: recs[cut].Seq - 1, OutputMessage: OutputMessage{
		E: fmt.Sprintf("[pipedpeer] output trimmed: the first %s were dropped to keep this log under %s\n",
			humanize.IBytes(uint64(l.size-kept)), humanize.IBytes(uint64(jobOutputCap))),
	}}
	var buf bytes.Buffer
	for _, r := range append([]storedOutput{note}, recs[cut:]...) {
		data, _ := json.Marshal(r)
		buf.Write(append(data, '\n'))
	}
	tmp := l.path + ".tmp"
	if err := os.WriteFile(tmp, buf.Bytes(), 0o644); err != nil {
		l.warnOnce(err, "trimming job output log; it keeps growing")
		return
	}
	if err := os.Rename(tmp, l.path); err != nil {
		l.warnOnce(err, "trimming job output log; it keeps growing")
		return
	}
	if l.f != nil {
		l.f.Close()
	}
	f, err := os.OpenFile(l.path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		l.f = nil
		l.warnOnce(err, "reopening trimmed job output log; attach will miss later output")
	} else {
		l.f = f
	}
	log.Warn().Str("job", l.jobID).Int("dropped_frames", cut).Msg("job output log over its cap; kept the newest half")
	l.size = int64(buf.Len())
}

// failOrphanedJobs runs once at daemon start, after the saved jobs are
// loaded. A job recorded as running lost its process (and the pipe its output
// came through) with the old daemon, so nothing would ever finish it: mark it
// failed, record a done frame that says why (attach ends with it) and free
// its lease. A crun container or an unisolated job's process group that
// outlived the daemon is stopped, since no one can collect its output.
func (s *Server) failOrphanedJobs() {
	var orphans []string
	type group struct {
		pgid  int
		start int64
	}
	groups := map[string]group{}
	s.jobsMu.Lock()
	for id, job := range s.jobs {
		if job.Status == "running" || job.Status == "importing" {
			job.Status = "failed"
			job.FinishedAt = time.Now()
			orphans = append(orphans, id)
			groups[id] = group{job.Pgid, job.PgidStart}
		}
	}
	s.jobsMu.Unlock()
	if len(orphans) == 0 {
		return
	}
	_, haveCrun := exec.LookPath("crun")
	for _, id := range orphans {
		if haveCrun == nil {
			ctr := "pp-" + id
			if exec.Command("crun", "state", ctr).Run() == nil {
				if out, err := exec.Command("crun", "delete", "-f", ctr).CombinedOutput(); err != nil {
					log.Warn().Err(err).Str("job", id).Str("output", string(out)).Msg("removing a job container left by the previous daemon")
				}
			}
		}
		l := s.jobOutput(id)
		if l.finalFrame() == nil {
			l.Finish(OutputMessage{ExitCode: -1, Error: "job lost: the daemon restarted while it ran"})
		}
		if g := groups[id]; g.pgid > 0 {
			killOrphanGroup(id, g.pgid, g.start)
		}
		s.releaseJobLease(id, "failed")
		log.Warn().Str("job", id).Msg("job was running when the daemon stopped; marked failed")
	}
	s.persist()
}

// recordJobGroup saves an unisolated job's process group and its leader's
// start time, so a restarted daemon can stop the job (failOrphanedJobs).
func (s *Server) recordJobGroup(job *JobRecord, pid int) {
	start, err := procStartTime(pid)
	if err != nil {
		log.Warn().Err(err).Str("job", job.JobID).Int("pid", pid).
			Msg("cannot read the job's start time; a daemon restart will not stop this job")
		return
	}
	s.jobsMu.Lock()
	job.Pgid, job.PgidStart = pid, start
	s.jobsMu.Unlock()
	s.persist()
}

// procStartTime is pid's start time in ms since the epoch.
func procStartTime(pid int) (int64, error) {
	p, err := process.NewProcess(int32(pid))
	if err != nil {
		return 0, err
	}
	return p.CreateTime()
}

// killOrphanGroup SIGKILLs the process group an unisolated job left behind
// when the daemon stopped, if its leader still runs. The leader's start time
// must match the recorded one: pids are reused, and a group led by a stranger
// is not ours to kill.
// ponytail: checks the leader only; members whose leader already exited keep
// running (python's own children normally die with it).
func killOrphanGroup(jobID string, pgid int, start int64) {
	now, err := procStartTime(pgid)
	if err != nil {
		log.Info().Str("job", jobID).Int("pgid", pgid).Msg("orphaned job's process already exited")
		return
	}
	if now != start {
		log.Warn().Str("job", jobID).Int("pgid", pgid).
			Msg("orphaned job's process id now belongs to another process; not stopping it")
		return
	}
	if err := syscall.Kill(-pgid, syscall.SIGKILL); err != nil && !errors.Is(err, syscall.ESRCH) {
		log.Warn().Err(err).Str("job", jobID).Int("pgid", pgid).Msg("stopping a job left running by the previous daemon")
		return
	}
	log.Warn().Str("job", jobID).Int("pgid", pgid).Msg("stopped a job left running by the previous daemon")
}
