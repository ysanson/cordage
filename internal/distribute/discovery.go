package distribute

import (
	"fmt"
	"net"
	"sort"
	"strconv"
)

// Discoverer resolves the current set of live worker addresses. It is
// called fresh before every distributed run (see CoordinatorServer.Query
// and cmd/cordage's coordinator command) so that whatever it returns
// reflects the caller's addressing scheme at that moment -- a fixed list
// for local/dev use, or a live lookup for a Kubernetes deployment whose
// worker replica count can change between calls.
type Discoverer func() ([]string, error)

// StaticDiscoverer wraps a fixed, pre-resolved address list -- the
// existing `-workers host:port,...` behavior, unchanged.
func StaticDiscoverer(addrs []string) Discoverer {
	return func() ([]string, error) { return addrs, nil }
}

// lookupHost is net.LookupHost by default; overridden in tests so
// DNSDiscoverer can be exercised without a real resolver.
var lookupHost = net.LookupHost

// DNSDiscoverer resolves host (typically a Kubernetes headless Service
// name, which returns one A record per ready backing pod) and pairs each
// resolved IP with port. Results are sorted so that repeated calls at a
// stable replica count produce a stable shard-to-worker assignment.
func DNSDiscoverer(host string, port int) Discoverer {
	return func() ([]string, error) {
		ips, err := lookupHost(host)
		if err != nil {
			return nil, fmt.Errorf("distribute: resolve %s: %w", host, err)
		}
		if len(ips) == 0 {
			return nil, fmt.Errorf("distribute: %s resolved to no addresses", host)
		}
		addrs := make([]string, len(ips))
		for i, ip := range ips {
			addrs[i] = net.JoinHostPort(ip, strconv.Itoa(port))
		}
		sort.Strings(addrs)
		return addrs, nil
	}
}
