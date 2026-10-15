package invite

import (
	"errors"
	"reflect"
	"testing"
)

func TestRoundTrip(t *testing.T) {
	in := Invite{Secret: "abc123", Addrs: []string{"10.0.0.1:4244", "[2001:db8::1]:4244"}}
	got, err := Decode(" " + Encode(in) + "\n")
	if err != nil || !reflect.DeepEqual(got, in) {
		t.Fatalf("got %+v, %v", got, err)
	}
}

func TestOneFlippedCharacterIsDamaged(t *testing.T) {
	code := Encode(Invite{Secret: "abc123", Addrs: []string{"10.0.0.1:4244"}})
	for i := len(prefix); i < len(code); i++ {
		b := []byte(code)
		if b[i] == 'A' {
			b[i] = 'B'
		} else {
			b[i] = 'A'
		}
		if _, err := Decode(string(b)); !errors.Is(err, ErrDamaged) {
			t.Fatalf("flip at %d: err = %v, want ErrDamaged", i, err)
		}
	}
}

func TestOtherVersionAndGarbage(t *testing.T) {
	if _, err := Decode("pp2-abcdef"); !errors.Is(err, ErrVersion) {
		t.Errorf("pp2-: %v", err)
	}
	if _, err := Decode("hello"); !errors.Is(err, ErrDamaged) {
		t.Errorf("garbage: %v", err)
	}
}
