// Package invite is the code `pipedpeer invite` prints and `connect` reads:
// the cluster secret plus a few introducer addresses, in one pasteable word.
package invite

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"strings"
)

// Invite is what a new machine needs to join: the secret and where to ask.
type Invite struct {
	Secret string   `json:"s"`
	Addrs  []string `json:"a"` // host:port of introducers
}

var (
	// ErrDamaged: the code was mistyped or cut short (checksum or JSON wrong).
	ErrDamaged = errors.New("invite code is damaged (copied wrongly or cut short)")
	// ErrVersion: the code is not a "pp1-" code, e.g. from a newer build.
	ErrVersion = errors.New("invite code is from a different version of pipedpeer")
)

const prefix = "pp1-"

// Encode returns "pp1-" + base64url(JSON + crc32 of the JSON).
func Encode(i Invite) string {
	b, _ := json.Marshal(i) // plain strings: cannot fail
	b = binary.BigEndian.AppendUint32(b, crc32.ChecksumIEEE(b))
	return prefix + base64.RawURLEncoding.EncodeToString(b)
}

// Decode reverses Encode. Surrounding whitespace is ignored.
func Decode(code string) (Invite, error) {
	code = strings.TrimSpace(code)
	if !strings.HasPrefix(code, prefix) {
		if len(code) > 3 && code[2] >= '0' && code[2] <= '9' && code[3] == '-' && strings.HasPrefix(code, "pp") {
			return Invite{}, ErrVersion
		}
		return Invite{}, ErrDamaged
	}
	b, err := base64.RawURLEncoding.DecodeString(code[len(prefix):])
	if err != nil || len(b) < 5 {
		return Invite{}, ErrDamaged
	}
	body, sum := b[:len(b)-4], binary.BigEndian.Uint32(b[len(b)-4:])
	if crc32.ChecksumIEEE(body) != sum {
		return Invite{}, ErrDamaged
	}
	var i Invite
	if err := json.Unmarshal(body, &i); err != nil || i.Secret == "" {
		return Invite{}, ErrDamaged
	}
	return i, nil
}
