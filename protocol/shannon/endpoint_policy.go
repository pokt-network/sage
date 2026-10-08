package shannon

import (
	"net"
	"strings"

	"github.com/pokt-network/sage/config"
	"github.com/pokt-network/sage/domain"
)

// blockedSupplierTable is the per-service set of supplier operator addresses an
// operator has decided never to select (config blocked_suppliers). Matched on
// the supplier address so it survives session rollover, and applied at the one
// place endpoints are handed out. Nil is a valid empty table.
type blockedSupplierTable map[domain.ServiceID]map[string]struct{}

// buildBlockedSuppliers lifts the per-service config lists into a lookup.
func buildBlockedSuppliers(services []config.ServiceConfig) blockedSupplierTable {
	var table blockedSupplierTable
	for _, svc := range services {
		if len(svc.BlockedSuppliers) == 0 {
			continue
		}
		if table == nil {
			table = make(blockedSupplierTable)
		}
		set := make(map[string]struct{}, len(svc.BlockedSuppliers))
		for _, s := range svc.BlockedSuppliers {
			if s != "" {
				set[s] = struct{}{}
			}
		}
		table[domain.ServiceID(svc.ID)] = set
	}
	return table
}

// blocked reports whether the supplier is blocked for the service.
func (t blockedSupplierTable) blocked(serviceID domain.ServiceID, supplier string) bool {
	_, ok := t[serviceID][supplier]
	return ok
}

// policyRejects reports whether the URL violates the gateway-wide endpoint URL
// policy (config endpoint_policy): a plaintext scheme when HTTPS is required,
// or a raw-IP host when a domain is required. The zero policy permits
// everything.
func policyRejects(p config.EndpointPolicy, url string) bool {
	if p.RequireHTTPS && !isSecureURL(url) {
		return true
	}
	if p.RequireDomain && isRawIPHost(hostOf(url)) {
		return true
	}
	return false
}

// isSecureURL reports whether the URL uses a TLS scheme (https or wss).
func isSecureURL(url string) bool {
	return strings.HasPrefix(url, "https://") || strings.HasPrefix(url, "wss://")
}

// hostOf returns the host (no scheme, path or port) of a URL.
func hostOf(url string) string {
	if i := strings.Index(url, "://"); i >= 0 {
		url = url[i+3:]
	}
	if i := strings.IndexByte(url, '/'); i >= 0 {
		url = url[:i]
	}
	// Strip a port, but not the colons of a bracketed IPv6 literal.
	if strings.HasPrefix(url, "[") {
		if i := strings.IndexByte(url, ']'); i >= 0 {
			return url[1:i]
		}
	}
	if i := strings.LastIndexByte(url, ':'); i >= 0 {
		url = url[:i]
	}
	return url
}

// isRawIPHost reports whether host is an IP literal rather than a domain name.
func isRawIPHost(host string) bool {
	return host != "" && net.ParseIP(host) != nil
}
