package portmap

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

// fakeRouter is a UPnP-IGD router on loopback: an SSDP responder and the two
// SOAP calls that matter.
//
// UPnP is the protocol most consumer routers actually enable, so it is the
// one most likely to run in the field - and the one that cannot be exercised
// at all without standing a router up, which is why it would otherwise go
// untested forever.
type fakeRouter struct {
	http     *httptest.Server
	ssdp     *net.UDPConn
	added    chan map[string]string
	deleted  chan string
	extIP    string
	failWith string // a UPnP error code, when the router should refuse
}

func startFakeRouter(t *testing.T, extIP string) *fakeRouter {
	t.Helper()
	r := &fakeRouter{
		added:   make(chan map[string]string, 4),
		deleted: make(chan string, 4),
		extIP:   extIP,
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/desc.xml", func(w http.ResponseWriter, _ *http.Request) {
		// The WAN service nested two levels down, as on a real router.
		fmt.Fprintf(w, `<?xml version="1.0"?><root><device>
  <deviceList><device>
    <deviceList><device><serviceList><service>
      <serviceType>urn:schemas-upnp-org:service:WANIPConnection:1</serviceType>
      <controlURL>/ctl</controlURL>
    </service></serviceList></device></deviceList>
  </device></deviceList>
</device></root>`)
	})
	mux.HandleFunc("/ctl", func(w http.ResponseWriter, req *http.Request) {
		raw, _ := io.ReadAll(io.LimitReader(req.Body, 1<<20))
		body := string(raw)
		action := req.Header.Get("SOAPAction")

		if r.failWith != "" {
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, `<s:Envelope><s:Body><s:Fault><detail><UPnPError>`+
				`<errorCode>%s</errorCode></UPnPError></detail></s:Fault></s:Body></s:Envelope>`, r.failWith)
			return
		}

		switch {
		case strings.Contains(action, "AddPortMapping"):
			r.added <- map[string]string{
				"external": betweenTag(body, "NewExternalPort"),
				"internal": betweenTag(body, "NewInternalPort"),
				"client":   betweenTag(body, "NewInternalClient"),
				"protocol": betweenTag(body, "NewProtocol"),
				"lease":    betweenTag(body, "NewLeaseDuration"),
				"desc":     betweenTag(body, "NewPortMappingDescription"),
			}
			fmt.Fprint(w, `<s:Envelope><s:Body><u:AddPortMappingResponse/></s:Body></s:Envelope>`)
		case strings.Contains(action, "DeletePortMapping"):
			r.deleted <- betweenTag(body, "NewExternalPort")
			fmt.Fprint(w, `<s:Envelope><s:Body><u:DeletePortMappingResponse/></s:Body></s:Envelope>`)
		case strings.Contains(action, "GetExternalIPAddress"):
			fmt.Fprintf(w, `<s:Envelope><s:Body><u:GetExternalIPAddressResponse>`+
				`<NewExternalIPAddress>%s</NewExternalIPAddress>`+
				`</u:GetExternalIPAddressResponse></s:Body></s:Envelope>`, r.extIP)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	})
	r.http = httptest.NewServer(mux)
	t.Cleanup(r.http.Close)

	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	r.ssdp = conn
	t.Cleanup(func() { conn.Close() })

	go func() {
		buf := make([]byte, 2048)
		for {
			n, from, err := conn.ReadFromUDP(buf)
			if err != nil {
				return
			}
			if !strings.Contains(string(buf[:n]), "M-SEARCH") {
				continue
			}
			resp := "HTTP/1.1 200 OK\r\nST: urn:schemas-upnp-org:service:WANIPConnection:1\r\n" +
				"LOCATION: " + r.http.URL + "/desc.xml\r\n\r\n"
			_, _ = conn.WriteToUDP([]byte(resp), from)
		}
	}()

	old := ssdpAddr
	ssdpAddr = conn.LocalAddr().String()
	t.Cleanup(func() { ssdpAddr = old })
	return r
}

// TestUPnPAddsTheMappingItWasAskedFor checks the whole path: multicast
// search, description fetch, nested service discovery, SOAP call.
func TestUPnPAddsTheMappingItWasAskedFor(t *testing.T) {
	r := startFakeRouter(t, "203.0.113.77")

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	ext, lifetime, err := upnpMap(ctx, 38447, time.Hour, "pipedpeer")
	if err != nil {
		t.Fatal(err)
	}
	if ext.String() != "203.0.113.77:38447" {
		t.Errorf("external = %s, want 203.0.113.77:38447", ext)
	}
	if lifetime != time.Hour {
		t.Errorf("lifetime = %s, want 1h", lifetime)
	}

	select {
	case got := <-r.added:
		if got["external"] != "38447" || got["internal"] != "38447" {
			t.Errorf("mapped %s -> %s, want 38447 -> 38447", got["external"], got["internal"])
		}
		if got["protocol"] != "UDP" {
			t.Errorf("protocol = %s, want UDP — TCP would forward the wrong thing entirely", got["protocol"])
		}
		if got["lease"] != "3600" {
			t.Errorf("lease = %s, want 3600", got["lease"])
		}
		// The client address must be this machine's address toward the
		// router. Naming the wrong interface points the mapping at a
		// container bridge or a tunnel, and the router forwards into
		// nothing.
		if got["client"] == "" || strings.HasPrefix(got["client"], "0.") {
			t.Errorf("client = %q, which is not an address on the way to the router", got["client"])
		}
	default:
		t.Fatal("the router was never asked to add a mapping")
	}
}

// TestUPnPRefusalIsReportedWithItsCode. 718 means the port belongs to
// somebody else and 401 means the router will not do this at all; a caller
// deciding whether to retry needs to be able to tell them apart.
func TestUPnPRefusalIsReportedWithItsCode(t *testing.T) {
	r := startFakeRouter(t, "203.0.113.77")
	r.failWith = "718"

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	_, _, err := upnpMap(ctx, 38447, time.Hour, "pipedpeer")
	if err == nil {
		t.Fatal("a refused mapping was reported as success")
	}
	if !strings.Contains(err.Error(), "718") {
		t.Errorf("error %q does not carry the UPnP code, so a reader cannot tell "+
			"a taken port from a disabled feature", err)
	}
}

// TestUPnPGivesThePortBack. Mappings left behind by every run accumulate on
// the router until it refuses new ones.
func TestUPnPGivesThePortBack(t *testing.T) {
	r := startFakeRouter(t, "203.0.113.77")

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	if err := upnpUnmap(ctx, 38447); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-r.deleted:
		if got != "38447" {
			t.Errorf("deleted port %s, want 38447", got)
		}
	default:
		t.Fatal("the router was never asked to delete the mapping")
	}
}

// TestADeviceWithoutTheWANServiceIsSkipped. Plenty of things answer an SSDP
// search - printers, media servers - and none of them can forward a port.
func TestADeviceWithoutTheWANServiceIsSkipped(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `<?xml version="1.0"?><root><device><serviceList><service>`+
			`<serviceType>urn:schemas-upnp-org:service:Printer:1</serviceType>`+
			`<controlURL>/ctl</controlURL></service></serviceList></device></root>`)
	}))
	defer srv.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := describeUPnP(ctx, srv.URL+"/desc.xml", wanServices[0])
	if err == nil {
		t.Error("a printer was accepted as a router")
	}
}
