package console

import (
	"errors"
	"net"
)

// fakeListener reports an address without binding it.
type fakeListener struct{ addr string }

func (l fakeListener) Accept() (net.Conn, error) { return nil, errors.New("closed") }
func (l fakeListener) Close() error              { return nil }
func (l fakeListener) Addr() net.Addr {
	a, _ := net.ResolveTCPAddr("tcp", l.addr)
	return a
}
