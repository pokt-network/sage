package cosmos

import (
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/pokt-network/sage/domain"
	"github.com/tidwall/gjson"
)

// Height-aware routing for CometBFT and Cosmos REST.
//
// A pruned node answers a query for a height it no longer holds with
// "height N is not available, lowest height is M" (CometBFT wraps it as
// JSON-RPC -32603, the gRPC-gateway REST face as an error message). That is
// the node's correct answer, so the heuristic passes it through: no retry,
// no penalty. What it must also be is remembered, or every later query for
// an old height goes back to the same pruned host. On the 2026-09-13 canary
// persistence's whole comet_bft face was one pruned host and clients asked
// for heights in the tens of thousands on a chain past 25M; retrying blindly
// on the other services (5bae566) exhausted every time because the other
// operators prune too, and cost 31–42% more paid relays.
//
// So the plugin learns each host's lowest held height from the answer and
// SelectEndpoints keeps hosts known to be pruned below the requested height
// out of the candidate set. When nothing in the pool holds the height the
// filter empties and the shared selector falls back to the full list: the
// query is sent once, the node's answer is delivered, nothing is retried.
// When any host holds it, it is found deterministically rather than by luck
// of rotation.
//
// The memory is keyed by host, not by supplier address: many staked
// addresses front one URL (rm02.kalorius.tech carried 229 on persistence),
// and pruning is a property of the node behind it. It expires, like the EVM
// plugin's archival marks: an operator may switch a host to an archive node,
// and a wrong mark must stay cheap.

// prunedTTL is how long one "lowest height is M" observation is trusted for
// a host. A pruned node's lowest height only rises, so a stale mark errs on
// the side of excluding a host that could serve; an hour bounds that cost to
// one re-learning relay per host per hour.
const prunedTTL = time.Hour

// maxPrunedHosts bounds the memory. Hosts come from staked URLs, so the set
// is small in practice; the cap is for a session churn nobody planned for.
const maxPrunedHosts = 4096

type prunedEntry struct {
	lowest uint64
	expiry time.Time
}

// prunedMemory remembers, per host, the lowest height the node reported
// holding. Safe for concurrent use.
type prunedMemory struct {
	mu      sync.RWMutex
	entries map[string]prunedEntry
	ttl     time.Duration
	max     int
	now     func() time.Time
}

func newPrunedMemory() *prunedMemory {
	return &prunedMemory{
		entries: make(map[string]prunedEntry),
		ttl:     prunedTTL,
		max:     maxPrunedHosts,
		now:     time.Now,
	}
}

// set records that host reported lowest as its lowest held height.
func (m *prunedMemory) set(host string, lowest uint64) {
	if host == "" || lowest == 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, known := m.entries[host]; !known && len(m.entries) >= m.max {
		// Clear wholesale rather than evict: the memory rebuilds at one
		// relay per host, and a map this size means something upstream is
		// producing hosts, not that the memory is worth preserving.
		m.entries = make(map[string]prunedEntry)
	}
	m.entries[host] = prunedEntry{lowest: lowest, expiry: m.now().Add(m.ttl)}
}

// lowest returns the host's remembered lowest height, if the observation has
// not aged out.
func (m *prunedMemory) lowest(host string) (uint64, bool) {
	m.mu.RLock()
	e, ok := m.entries[host]
	m.mu.RUnlock()
	if !ok || !m.now().Before(e.expiry) {
		return 0, false
	}
	return e.lowest, true
}

// reset forgets everything.
func (m *prunedMemory) reset() {
	m.mu.Lock()
	m.entries = make(map[string]prunedEntry)
	m.mu.Unlock()
}

// lowestHeightRe matches CometBFT's pruned-height wording, whichever field
// carries it: the JSON-RPC -32603 `data` on the RPC face, the gRPC-gateway
// error `message` on the REST face.
var lowestHeightRe = regexp.MustCompile(`lowest height is (\d+)`)

// prunedLowestHeight reads the lowest held height out of a pruned node's
// answer. ok is false for any other body.
func prunedLowestHeight(response []byte) (lowest uint64, ok bool) {
	if len(response) == 0 {
		return 0, false
	}
	if len(response) > 2048 {
		response = response[:2048]
	}
	m := lowestHeightRe.FindSubmatch(response)
	if m == nil {
		return 0, false
	}
	v, err := strconv.ParseUint(string(m[1]), 10, 64)
	if err != nil || v == 0 {
		return 0, false
	}
	return v, true
}

// heightMethods are the CometBFT methods whose `height` parameter names a
// specific block. `blockchain` takes a range and is read from minHeight.
var heightMethods = map[string]bool{
	"block":            true,
	"block_results":    true,
	"commit":           true,
	"validators":       true,
	"consensus_params": true,
	"header":           true,
	"abci_query":       true,
}

// requestedHeight returns the specific block height a request names, on any
// face: a CometBFT JSON-RPC body ({"method":"block","params":{"height":"5"}}
// or positional ["5"]), a CometBFT GET (/block?height=5), or a Cosmos REST
// path (/cosmos/base/tendermint/v1beta1/blocks/5, /cosmos/tx/v1beta1/txs/block/5,
// /cosmos/base/tendermint/v1beta1/validatorsets/5). ok is false when the
// request asks for the latest block or names no height: those need no
// history and must not be filtered.
func requestedHeight(p domain.Payload) (height uint64, ok bool) {
	if m := p.Method(); m != "" {
		return heightFromJSONRPC(m, p.Bytes())
	}
	return heightFromPath(p.Path())
}

func heightFromJSONRPC(method string, body []byte) (uint64, bool) {
	method = strings.ToLower(method)
	params := gjson.GetBytes(body, "params")
	if !params.Exists() {
		return 0, false
	}
	switch {
	case method == "blockchain":
		return parseHeightValue(params.Get("minHeight"))
	case heightMethods[method]:
		if params.IsObject() {
			return parseHeightValue(params.Get("height"))
		}
		if params.IsArray() {
			arr := params.Array()
			if len(arr) > 0 {
				return parseHeightValue(arr[0])
			}
		}
	}
	return 0, false
}

func heightFromPath(rawPath string) (uint64, bool) {
	if rawPath == "" {
		return 0, false
	}
	u, err := url.ParseRequestURI(rawPath)
	if err != nil {
		return 0, false
	}
	seg := strings.TrimPrefix(u.Path, "/")
	if i := strings.IndexByte(seg, '/'); i >= 0 {
		seg = seg[:i]
	}
	seg = strings.ToLower(seg)
	// CometBFT GET face: the method is the first segment, the height a query
	// parameter.
	if seg == "blockchain" {
		return parseHeightString(u.Query().Get("minHeight"))
	}
	if heightMethods[seg] {
		return parseHeightString(u.Query().Get("height"))
	}
	// Cosmos REST face: the height is the last path segment of the routes
	// that take one.
	for _, prefix := range restHeightPrefixes {
		if rest, found := strings.CutPrefix(u.Path, prefix); found {
			if i := strings.IndexByte(rest, '/'); i >= 0 {
				rest = rest[:i]
			}
			return parseHeightString(rest)
		}
	}
	return 0, false
}

// restHeightPrefixes are the Cosmos SDK REST routes whose next path segment
// is a block height. "latest" in that position is the current block.
var restHeightPrefixes = []string{
	"/cosmos/base/tendermint/v1beta1/blocks/",
	"/cosmos/base/tendermint/v1beta1/validatorsets/",
	"/cosmos/tx/v1beta1/txs/block/",
}

func parseHeightValue(v gjson.Result) (uint64, bool) {
	switch v.Type {
	case gjson.String:
		return parseHeightString(v.String())
	case gjson.Number:
		if v.Num <= 0 {
			return 0, false
		}
		return uint64(v.Num), true
	default:
		return 0, false
	}
}

// parseHeightString reads a decimal height. Empty, "latest" and "0" all
// mean the current block: no history required.
func parseHeightString(s string) (uint64, bool) {
	s = strings.TrimSpace(s)
	if s == "" || strings.EqualFold(s, "latest") {
		return 0, false
	}
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil || v == 0 {
		return 0, false
	}
	return v, true
}
