package phone

import (
	"encoding/binary"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/rs/zerolog/log"
)

// ImportsHeader lists the top-level modules a chunk needs ("torch,numpy") on a
// /v1/store?runnable=1 request, so a phone can refuse one it cannot import.
const ImportsHeader = "X-Pipedpeer-Imports"

var (
	importRe = regexp.MustCompile(`(?m)^[ \t]*import[ \t]+([^\n#;]+)`)
	fromRe   = regexp.MustCompile(`(?m)^[ \t]*from[ \t]+([A-Za-z_][\w.]*)[ \t]+import\b`)
)

// Modules returns the sorted top-level modules a chunk needs: the import
// lines of its func_src plus every module its pickles reference (GLOBAL and
// STACK_GLOBAL opcodes, and cloudpickle's subimport("x") arguments).
// __main__ and builtins are dropped: every python has them.
func Modules(funcSrc string, pickles ...[]byte) []string {
	set := map[string]bool{}
	add := func(m string) {
		m = strings.TrimSpace(m)
		if i := strings.IndexByte(m, '.'); i >= 0 {
			m = m[:i]
		}
		if m != "" && m != "__main__" && m != "builtins" && m != "__builtin__" {
			set[m] = true
		}
	}
	for _, g := range importRe.FindAllStringSubmatch(funcSrc, -1) {
		for _, part := range strings.Split(g[1], ",") {
			if f := strings.Fields(part); len(f) > 0 {
				add(f[0])
			}
		}
	}
	for _, g := range fromRe.FindAllStringSubmatch(funcSrc, -1) {
		add(g[1])
	}
	for _, p := range pickles {
		pickleModules(p, add)
	}
	out := make([]string, 0, len(set))
	for m := range set {
		out = append(out, m)
	}
	sort.Strings(out)
	return out
}

// pickleModules walks pickle opcodes without executing anything and reports
// each module name it references. It stops quietly at the end of the data and
// with a log line at an opcode it does not know (a later protocol): the names
// found so far still count.
//
// DECIDED: hand-rolled: the Go pickle library (nlpodyssey/gopickle) unpickles
// whole objects (reduce calls, tensor storage); this needs opcode names only.
func pickleModules(b []byte, add func(string)) {
	var (
		strs    []string // recent pushes; "" for a non-string value
		memo    = map[uint64]string{}
		nextMem uint64
		sub     bool // last global was cloudpickle's subimport
	)
	push := func(s string) {
		if sub && s != "" {
			add(s)
			sub = false
		}
		strs = append(strs, s)
		if len(strs) > 8 {
			strs = strs[1:]
		}
	}
	top := func() string {
		if len(strs) == 0 {
			return ""
		}
		return strs[len(strs)-1]
	}
	global := func(mod, name string) {
		add(mod)
		sub = name == "subimport" || name == "dynamic_subimport"
		push("")
	}
	line := func(i int) (string, int, bool) {
		j := i
		for j < len(b) && b[j] != '\n' {
			j++
		}
		if j >= len(b) {
			return "", 0, false
		}
		return string(b[i:j]), j + 1, true
	}
	n := func(i, size int) (uint64, bool) {
		if i+size > len(b) {
			return 0, false
		}
		switch size {
		case 1:
			return uint64(b[i]), true
		case 2:
			return uint64(binary.LittleEndian.Uint16(b[i:])), true
		case 4:
			return uint64(binary.LittleEndian.Uint32(b[i:])), true
		}
		return binary.LittleEndian.Uint64(b[i:]), true
	}
	i := 0
	for i < len(b) {
		op := b[i]
		i++
		switch op {
		case '.': // STOP: several pickles may be concatenated
			strs = strs[:0]
		case 'c', 'i': // GLOBAL, INST: two text lines
			mod, j, ok := line(i)
			if !ok {
				return
			}
			name, k, ok := line(j)
			if !ok {
				return
			}
			global(mod, name)
			i = k
		case 0x93: // STACK_GLOBAL
			if len(strs) >= 2 {
				global(strs[len(strs)-2], strs[len(strs)-1])
			} else {
				push("")
			}
		case 0x94: // MEMOIZE
			memo[nextMem] = top()
			nextMem++
		case 'I', 'L', 'F', 'P', 'S', 'V', 'g', 'p': // newline-terminated argument
			s, j, ok := line(i)
			if !ok {
				return
			}
			switch op {
			case 'p':
				if k, err := parseUint(s); err == nil {
					memo[k] = top()
				}
			case 'g':
				k, _ := parseUint(s)
				push(memo[k])
			case 'S':
				push(strings.Trim(s, `'"`))
			case 'V':
				push(s)
			default:
				push("")
			}
			i = j
		case 'K', 'M', 'J', 0x80, 0x82, 0x83, 0x84, 'G', 0x95: // fixed-size argument
			size := map[byte]int{'K': 1, 'M': 2, 'J': 4, 0x80: 1, 0x82: 1, 0x83: 2, 0x84: 4, 'G': 8, 0x95: 8}[op]
			if i+size > len(b) {
				return
			}
			if op != 0x80 && op != 0x95 {
				push("")
			}
			i += size
		case 'h', 'j', 'q', 'r': // BINGET, LONG_BINGET, BINPUT, LONG_BINPUT
			size := 1
			if op == 'j' || op == 'r' {
				size = 4
			}
			k, ok := n(i, size)
			if !ok {
				return
			}
			if op == 'h' || op == 'j' {
				push(memo[k])
			} else {
				memo[k] = top()
			}
			i += size
		case 'U', 0x8c, 'C', 0x8a, 'T', 'X', 'B', 0x8b, 0x8d, 0x8e, 0x96: // length-prefixed data
			size := 4
			switch op {
			case 'U', 0x8c, 'C', 0x8a:
				size = 1
			case 0x8d, 0x8e, 0x96:
				size = 8
			}
			l, ok := n(i, size)
			if !ok || uint64(len(b)-i-size) < l {
				return
			}
			data := b[i+size : i+size+int(l)]
			if op == 'U' || op == 0x8c || op == 'T' || op == 'X' || op == 0x8d {
				push(string(data))
			} else {
				push("")
			}
			i += size + int(l)
		case '(', '0', '1', '2', 'N', 'R', 'a', 'b', 'd', '}', 'e', 'l', ']', 'o', 's', 't', ')', 'u', 'Q',
			0x81, 0x85, 0x86, 0x87, 0x88, 0x89, 0x8f, 0x90, 0x91, 0x92, 0x97, 0x98: // no argument
			if op != '(' && op != 's' && op != 'u' && op != 'a' && op != 'e' && op != 'b' && op != '0' && op != '1' {
				push("")
			}
		default:
			log.Warn().Int("opcode", int(op)).Int("offset", i-1).Msg("phone: unknown pickle opcode; module scan stopped early")
			return
		}
	}
}

func parseUint(s string) (uint64, error) { return strconv.ParseUint(strings.TrimSpace(s), 10, 64) }
