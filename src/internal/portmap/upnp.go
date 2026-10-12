package portmap

import (
	"context"
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"time"
)

// UPnP-IGD is the oldest of the three and the one most consumer routers
// actually ship, so it is worth the XML. Only the two calls that matter are
// implemented - add a mapping, and ask what the external address is - rather
// than a general UPnP stack: everything else in the specification is a
// liability on a listener facing the LAN.

// ssdpAddr is where a router announces itself. A variable so tests can point
// the search at a loopback fake.
var ssdpAddr = "239.255.255.250:1900"

// wanServices are the two service types that can add a port mapping. A
// router offers one or the other depending on whether its uplink is routed
// or PPP.
var wanServices = []string{
	"urn:schemas-upnp-org:service:WANIPConnection:1",
	"urn:schemas-upnp-org:service:WANPPPConnection:1",
}

// upnpDevice is a router that answered, reduced to what is needed to call it.
type upnpDevice struct {
	controlURL  string
	serviceType string
}

// upnpMap adds a mapping and reports the external address.
//
// UPnP has no lease negotiation worth the name: the router takes the
// requested duration or ignores it, and there is no field in the reply to
// say which. The requested lifetime is returned, and the caller renews on
// that schedule - which is correct either way, since renewing a mapping that
// never expired is harmless.
func upnpMap(ctx context.Context, internal uint16, lifetime time.Duration, desc string) (netip.AddrPort, time.Duration, error) {
	dev, err := discoverUPnP(ctx)
	if err != nil {
		return netip.AddrPort{}, 0, err
	}

	local, err := localAddrToward(dev.controlURL)
	if err != nil {
		return netip.AddrPort{}, 0, err
	}

	body := fmt.Sprintf(`<NewRemoteHost></NewRemoteHost>`+
		`<NewExternalPort>%d</NewExternalPort>`+
		`<NewProtocol>UDP</NewProtocol>`+
		`<NewInternalPort>%d</NewInternalPort>`+
		`<NewInternalClient>%s</NewInternalClient>`+
		`<NewEnabled>1</NewEnabled>`+
		`<NewPortMappingDescription>%s</NewPortMappingDescription>`+
		`<NewLeaseDuration>%d</NewLeaseDuration>`,
		internal, internal, local, xmlEscape(desc), int(lifetime.Seconds()))

	if _, err := soap(ctx, dev, "AddPortMapping", body); err != nil {
		return netip.AddrPort{}, 0, err
	}

	extBody, err := soap(ctx, dev, "GetExternalIPAddress", "")
	if err != nil {
		return netip.AddrPort{}, 0, fmt.Errorf("mapping added but the router would not say its external address: %w", err)
	}
	ipStr := betweenTag(extBody, "NewExternalIPAddress")
	ext, err := netip.ParseAddr(strings.TrimSpace(ipStr))
	if err != nil {
		return netip.AddrPort{}, 0, fmt.Errorf("router reported external address %q: %w", ipStr, err)
	}
	// The requested external port is what was asked for; UPnP's AddPortMapping
	// either grants exactly that or fails, unlike the other two protocols
	// which may substitute one.
	return netip.AddrPortFrom(ext, internal), lifetime, nil
}

// upnpUnmap removes a mapping. Best effort: a router that has forgotten the
// mapping already answers with an error that means the same as success.
func upnpUnmap(ctx context.Context, internal uint16) error {
	dev, err := discoverUPnP(ctx)
	if err != nil {
		return err
	}
	body := fmt.Sprintf(`<NewRemoteHost></NewRemoteHost>`+
		`<NewExternalPort>%d</NewExternalPort>`+
		`<NewProtocol>UDP</NewProtocol>`, internal)
	_, err = soap(ctx, dev, "DeletePortMapping", body)
	return err
}

// discoverUPnP finds a router that can add a mapping.
func discoverUPnP(ctx context.Context) (upnpDevice, error) {
	for _, svc := range wanServices {
		locations, err := ssdpSearch(ctx, svc)
		if err != nil {
			continue
		}
		for _, loc := range locations {
			dev, err := describeUPnP(ctx, loc, svc)
			if err == nil {
				return dev, nil
			}
		}
	}
	return upnpDevice{}, fmt.Errorf("no router answered a UPnP search")
}

// ssdpSearch multicasts an M-SEARCH and collects the description URLs.
func ssdpSearch(ctx context.Context, service string) ([]string, error) {
	raddr, err := net.ResolveUDPAddr("udp4", ssdpAddr)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{})
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	msg := "M-SEARCH * HTTP/1.1\r\n" +
		"HOST: " + ssdpAddr + "\r\n" +
		`MAN: "ssdp:discover"` + "\r\n" +
		"MX: 2\r\n" +
		"ST: " + service + "\r\n\r\n"
	if _, err := conn.WriteToUDP([]byte(msg), raddr); err != nil {
		return nil, err
	}

	deadline := time.Now().Add(2 * time.Second)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	_ = conn.SetReadDeadline(deadline)

	var out []string
	buf := make([]byte, 2048)
	for {
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			break // deadline: whatever answered, answered
		}
		if loc := headerValue(string(buf[:n]), "LOCATION"); loc != "" {
			out = append(out, loc)
		}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("nothing answered the UPnP search")
	}
	return out, nil
}

// describeUPnP fetches the device description and finds the control URL for
// the service that can add mappings.
func describeUPnP(ctx context.Context, location, service string) (upnpDevice, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", location, nil)
	if err != nil {
		return upnpDevice{}, err
	}
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		return upnpDevice{}, err
	}
	defer resp.Body.Close()
	// Bounded: this is an unauthenticated device on the LAN describing
	// itself, and it should not be able to make this process allocate.
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return upnpDevice{}, err
	}

	var root struct {
		Device struct {
			ServiceList []struct {
				ServiceType string `xml:"serviceType"`
				ControlURL  string `xml:"controlURL"`
			} `xml:"serviceList>service"`
			DeviceList []struct {
				ServiceList []struct {
					ServiceType string `xml:"serviceType"`
					ControlURL  string `xml:"controlURL"`
				} `xml:"serviceList>service"`
				DeviceList []struct {
					ServiceList []struct {
						ServiceType string `xml:"serviceType"`
						ControlURL  string `xml:"controlURL"`
					} `xml:"serviceList>service"`
				} `xml:"deviceList>device"`
			} `xml:"deviceList>device"`
		} `xml:"device"`
	}
	if err := xml.Unmarshal(body, &root); err != nil {
		return upnpDevice{}, err
	}

	// The WAN service is nested two levels down on every real router, but
	// the nesting is not guaranteed, so all three levels are searched.
	var control string
	check := func(t, u string) {
		if control == "" && t == service {
			control = u
		}
	}
	for _, s := range root.Device.ServiceList {
		check(s.ServiceType, s.ControlURL)
	}
	for _, d := range root.Device.DeviceList {
		for _, s := range d.ServiceList {
			check(s.ServiceType, s.ControlURL)
		}
		for _, d2 := range d.DeviceList {
			for _, s := range d2.ServiceList {
				check(s.ServiceType, s.ControlURL)
			}
		}
	}
	if control == "" {
		return upnpDevice{}, fmt.Errorf("device at %s offers no %s", location, service)
	}

	base, err := url.Parse(location)
	if err != nil {
		return upnpDevice{}, err
	}
	ref, err := url.Parse(control)
	if err != nil {
		return upnpDevice{}, err
	}
	return upnpDevice{controlURL: base.ResolveReference(ref).String(), serviceType: service}, nil
}

// soap makes one SOAP call and returns the body.
func soap(ctx context.Context, dev upnpDevice, action, inner string) (string, error) {
	env := `<?xml version="1.0"?>` +
		`<s:Envelope xmlns:s="http://schemas.xmlsoap.org/soap/envelope/" ` +
		`s:encodingStyle="http://schemas.xmlsoap.org/soap/encoding/">` +
		`<s:Body><u:` + action + ` xmlns:u="` + dev.serviceType + `">` +
		inner +
		`</u:` + action + `></s:Body></s:Envelope>`

	req, err := http.NewRequestWithContext(ctx, "POST", dev.controlURL, strings.NewReader(env))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", `text/xml; charset="utf-8"`)
	req.Header.Set("SOAPAction", `"`+dev.serviceType+`#`+action+`"`)

	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		// The UPnP error code says whether this is worth reporting to the
		// user. 718 is "that mapping belongs to somebody else", which on a
		// shared network is ordinary; 401/501 mean the router will not do
		// this at all.
		if code := betweenTag(string(body), "errorCode"); code != "" {
			return "", fmt.Errorf("router refused %s: UPnP error %s", action, code)
		}
		return "", fmt.Errorf("router refused %s: HTTP %d", action, resp.StatusCode)
	}
	return string(body), nil
}

// localAddrToward is the address this machine has on the way to the router,
// which is the one a mapping must point at. Taking the first interface
// address instead would name the wrong interface on any machine with more
// than one - a laptop on Wi-Fi with a container bridge up, for instance.
func localAddrToward(controlURL string) (string, error) {
	u, err := url.Parse(controlURL)
	if err != nil {
		return "", err
	}
	host := u.Hostname()
	port := u.Port()
	if port == "" {
		port = "80"
	}
	// UDP: no packet is sent, but the kernel picks the source address it
	// would use, which is exactly the question being asked.
	conn, err := net.Dial("udp", net.JoinHostPort(host, port))
	if err != nil {
		return "", err
	}
	defer conn.Close()
	return conn.LocalAddr().(*net.UDPAddr).IP.String(), nil
}

func headerValue(resp, key string) string {
	for _, line := range strings.Split(resp, "\n") {
		k, v, ok := strings.Cut(line, ":")
		if ok && strings.EqualFold(strings.TrimSpace(k), key) {
			return strings.TrimSpace(v)
		}
	}
	return ""
}

// betweenTag pulls one value out of a SOAP body. A full parse would need the
// response schema for every action; the values wanted here are single
// unnested strings.
func betweenTag(body, tag string) string {
	open := "<" + tag + ">"
	i := strings.Index(body, open)
	if i < 0 {
		return ""
	}
	rest := body[i+len(open):]
	j := strings.Index(rest, "</"+tag+">")
	if j < 0 {
		return ""
	}
	return rest[:j]
}

func xmlEscape(s string) string {
	var b strings.Builder
	_ = xml.EscapeText(&b, []byte(s))
	return b.String()
}
