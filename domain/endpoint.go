package domain

import (
	"fmt"
	"strings"
)

// EndpointAddr uniquely identifies an endpoint (format: "supplier-url").
type EndpointAddr string

// EndpointAddrList is an ordered list of endpoint addresses.
type EndpointAddrList []EndpointAddr

// Supplier extracts the supplier address from the endpoint addr.
// Format: "pokt1abc...-https://example.com"
func (e EndpointAddr) Supplier() string {
	s := string(e)
	idx := strings.Index(s, "-")
	if idx < 0 {
		return s
	}
	return s[:idx]
}

// URL extracts the URL from the endpoint addr.
func (e EndpointAddr) URL() (string, error) {
	s := string(e)
	idx := strings.Index(s, "-")
	if idx < 0 || idx+1 >= len(s) {
		return "", fmt.Errorf("invalid endpoint addr format: %s", s)
	}
	return s[idx+1:], nil
}

// Domain extracts the domain/host from the endpoint URL.
func (e EndpointAddr) Domain() string {
	url, err := e.URL()
	if err != nil {
		return ""
	}
	// Strip scheme
	if idx := strings.Index(url, "://"); idx >= 0 {
		url = url[idx+3:]
	}
	// Strip path
	if idx := strings.IndexByte(url, '/'); idx >= 0 {
		url = url[:idx]
	}
	// Strip port
	if idx := strings.LastIndexByte(url, ':'); idx >= 0 {
		url = url[:idx]
	}
	return url
}

// Contains returns true if the list contains the given addr.
func (l EndpointAddrList) Contains(addr EndpointAddr) bool {
	for _, a := range l {
		if a == addr {
			return true
		}
	}
	return false
}

// Exclude returns a new list without the given addrs.
func (l EndpointAddrList) Exclude(addrs map[EndpointAddr]bool) EndpointAddrList {
	if len(addrs) == 0 {
		return l
	}
	out := make(EndpointAddrList, 0, len(l))
	for _, a := range l {
		if !addrs[a] {
			out = append(out, a)
		}
	}
	return out
}
