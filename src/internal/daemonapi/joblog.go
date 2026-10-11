package daemonapi

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"

	"github.com/rs/zerolog/log"
)

// jobOutputFile is the daemon's own record of everything a job sent its
// client, one storedOutput per line, so `attach` replays exactly what a live
// client saw.
const jobOutputFile = "output.jsonl"

// attachBuffer is how many frames a live attacher may fall behind before it
// is cut loose to catch up from the file.
const attachBuffer = 256

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
	done     bool
	final    *OutputMessage
	subs     map[chan storedOutput]struct{}
	warned   bool
	cancelFn func()
}

// jobLogs holds the open logs, keyed by file path (unique per daemon, since
// every Server has its own jobDir).
var jobLogs sync.Map

// jobOutput returns the output log of jobID, opening (or creating) its file.
// A log reopened after a daemon restart continues the file's sequence.
func (s *Server) jobOutput(jobID string) *jobOutputLog {
	path := filepath.Join(s.jobDir, jobID, jobOutputFile)
	if l, ok := jobLogs.Load(path); ok {
		return l.(*jobOutputLog)
	}
	l := &jobOutputLog{jobID: jobID, path: path, subs: map[chan storedOutput]struct{}{}}
	for _, m := range readStoredOutput(path, 0) {
		l.seq = m.Seq
		if m.Done {
			l.done = true
			final := m.OutputMessage
			l.final = &final
		}
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
		if _, err := l.f.Write(append(data, '\n')); err != nil {
			l.warnOnce(err, "writing job output log; attach will miss this output")
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
