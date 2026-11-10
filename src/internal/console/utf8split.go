package console

import "unicode/utf8"

// splitValidUTF8 returns b as text, holding back a trailing multi-byte
// character that a read boundary cut in half so it can be completed by the
// next read. Bytes that are simply invalid are passed through.
func splitValidUTF8(b []byte) (text string, rest []byte) {
	for cut := 1; cut < utf8.UTFMax && cut <= len(b); cut++ {
		start := len(b) - cut
		if !utf8.RuneStart(b[start]) {
			continue
		}
		if utf8.FullRune(b[start:]) {
			break
		}
		return string(b[:start]), append([]byte(nil), b[start:]...)
	}
	return string(b), nil
}
