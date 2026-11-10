package console

import (
	"bufio"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/pipedpeer/pipedpeer/internal/termuxexec"
)

//go:embed kernel/driver.py
var driverSource []byte

const (
	// The driver's ends of the protocol pipes, as numbered in the child:
	// ExtraFiles start at descriptor 3.
	driverControlFD = 3
	driverEventFD   = 4
	// How much of the interpreter's own stderr is kept to explain a kernel
	// that died before or outside the driver's output capture.
	stderrTailBytes = 8 << 10
	// How long Wait lingers for the interpreter's stderr after it exits.
	exitWaitDelay = 2 * time.Second
)

// kernelProc is one running Python driver process.
type kernelProc struct {
	cmd     *exec.Cmd
	control *os.File
	events  *os.File
	stderr  *tailBuffer

	writeMu sync.Mutex
}

// startKernelProc launches the driver in dir. onEvent is called for each
// driver event in order, then onExit exactly once when the process is gone.
func startKernelProc(python, driverPath, dir string, env []string, onEvent func(driverEvent), onExit func(error)) (*kernelProc, error) {
	controlR, controlW, err := os.Pipe()
	if err != nil {
		return nil, err
	}
	eventR, eventW, err := os.Pipe()
	if err != nil {
		controlR.Close()
		controlW.Close()
		return nil, err
	}

	tail := &tailBuffer{limit: stderrTailBytes}
	cmd := termuxexec.Command(python, "-u", driverPath, fmt.Sprint(driverControlFD), fmt.Sprint(driverEventFD))
	cmd.Dir = dir
	cmd.Env = env
	cmd.Stdout = tail
	cmd.Stderr = tail
	cmd.ExtraFiles = []*os.File{controlR, eventW}
	// Its own process group, so an interrupt reaches the cell's child
	// processes and a kill leaves none behind.
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.WaitDelay = exitWaitDelay

	err = cmd.Start()
	controlR.Close()
	eventW.Close()
	if err != nil {
		controlW.Close()
		eventR.Close()
		return nil, err
	}

	p := &kernelProc{cmd: cmd, control: controlW, events: eventR, stderr: tail}
	eventsDone := make(chan struct{})
	go func() {
		defer close(eventsDone)
		p.readEvents(onEvent)
	}()
	go func() {
		waitErr := cmd.Wait()
		// Children forked by a cell can hold the event pipe open after the
		// kernel is gone, so the reader is stopped rather than waited out.
		p.events.Close()
		p.control.Close()
		<-eventsDone
		onExit(waitErr)
	}()
	return p, nil
}

func (p *kernelProc) readEvents(onEvent func(driverEvent)) {
	r := bufio.NewReaderSize(p.events, 64<<10)
	for {
		line, err := r.ReadBytes('\n')
		if len(line) > 0 {
			var ev driverEvent
			if json.Unmarshal(line, &ev) == nil && ev.Type != "" {
				onEvent(ev)
			}
		}
		if err != nil {
			return
		}
	}
}

func (p *kernelProc) send(c driverCommand) error {
	line, err := json.Marshal(c)
	if err != nil {
		return err
	}
	p.writeMu.Lock()
	defer p.writeMu.Unlock()
	_, err = p.control.Write(append(line, '\n'))
	return err
}

func (p *kernelProc) signalGroup(sig syscall.Signal) {
	if p.cmd.Process != nil {
		_ = syscall.Kill(-p.cmd.Process.Pid, sig)
	}
}

func (p *kernelProc) interrupt() { p.signalGroup(syscall.SIGINT) }
func (p *kernelProc) kill()      { p.signalGroup(syscall.SIGKILL) }

// describeExit explains why the process ended, including anything the
// interpreter itself printed.
func (p *kernelProc) describeExit(waitErr error) string {
	msg := "kernel process exited"
	var exitErr *exec.ExitError
	switch {
	case errors.As(waitErr, &exitErr):
		msg = "kernel process ended: " + exitErr.ProcessState.String()
	case waitErr != nil:
		msg = "kernel process ended: " + waitErr.Error()
	}
	if tail := strings.TrimSpace(p.stderr.String()); tail != "" {
		msg += "\n" + tail
	}
	return msg
}

// tailBuffer keeps the last limit bytes written to it.
type tailBuffer struct {
	mu    sync.Mutex
	limit int
	buf   []byte
}

func (t *tailBuffer) Write(b []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.buf = append(t.buf, b...)
	if over := len(t.buf) - t.limit; over > 0 {
		t.buf = append(t.buf[:0], t.buf[over:]...)
	}
	return len(b), nil
}

func (t *tailBuffer) String() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return string(t.buf)
}
