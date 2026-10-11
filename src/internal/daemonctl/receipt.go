package daemonctl

import "strings"

// receiptPrefix starts the one stderr line the node's shim writes at exit with
// what ran where (C-RCPT). It is for the run summary, not for the terminal.
const receiptPrefix = "[pipedpeer] receipt "

// receiptFilter hides receipt lines from a raw stderr stream while letting
// everything else, \r progress redraws included, through the moment it
// arrives. Only text that is, or could still become, a receipt line is held,
// and only until its newline. A line starts at the beginning of the stream or
// after \n or \r (a progress bar may leave its line unterminated).
type receiptFilter struct {
	held    string
	midLine bool
}

// filter returns the part of chunk to print now and any complete receipt
// lines (without their newline).
func (f *receiptFilter) filter(chunk string) (show string, receipts []string) {
	data := f.held + chunk
	f.held = ""
	var out strings.Builder
	for i := 0; i < len(data); {
		rest := data[i:]
		if !f.midLine {
			if strings.HasPrefix(rest, receiptPrefix) {
				nl := strings.IndexByte(rest, '\n')
				if nl < 0 {
					f.held = rest
					break
				}
				receipts = append(receipts, rest[:nl])
				i += nl + 1
				continue
			}
			if strings.HasPrefix(receiptPrefix, rest) {
				// Too short to tell yet.
				f.held = rest
				break
			}
		}
		j := strings.IndexAny(rest, "\r\n")
		if j < 0 {
			out.WriteString(rest)
			f.midLine = true
			break
		}
		out.WriteString(rest[:j+1])
		f.midLine = false
		i += j + 1
	}
	return out.String(), receipts
}

// flush releases whatever is still held at the end of the stream: a receipt
// cut off without its newline, or text that only looked like one.
func (f *receiptFilter) flush() string {
	s := f.held
	f.held = ""
	return s
}
