package shannon

import (
	"context"
	"errors"
	"sync"
	"testing"

	apptypes "github.com/pokt-network/poktroll/x/application/types"
	servicetypes "github.com/pokt-network/poktroll/x/service/types"
	sessiontypes "github.com/pokt-network/poktroll/x/session/types"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/featureflag"
)

// sessionRecordingSigner records the session every request it signs names.
type sessionRecordingSigner struct {
	mu       sync.Mutex
	sessions []string
}

func (s *sessionRecordingSigner) signRelayRequest(_ context.Context, req *servicetypes.RelayRequest, _ *apptypes.Application) (*servicetypes.RelayRequest, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.sessions = append(s.sessions, req.Meta.SessionHeader.SessionId)
	return req, nil
}

// One block past the session's end, inside grace: HTTP still serves the ended
// session (both miners honour it), but a WebSocket probe must sign the next
// one, which the poktroll miner binds a new connection to.
func TestWSProbe_SignsTheSessionAtTheCurrentHeight(t *testing.T) {
	enabled := map[string]bool{featureflag.FlagWebsocketRelays: true, featureflag.FlagWebsocketProbes: true}
	supplier := newEchoSupplier(t)
	r, _, _ := probeFixture(t, wsURL(supplier), 40, `{"jsonrpc":"2.0","id":1,"result":"0x10"}`, enabled)
	p := r.deps.Protocol
	signer := &sessionRecordingSigner{}
	p.signer = signer
	fn := p.fullNode.(*mockRelayFullNode)

	p.sessions.graceBlocks.Store(10)
	// Cache the session ending at 110.
	p.sessions.latestBlockHeight.Store(105)
	ended, err := p.sessions.getSession(context.Background(), "eth", "pokt1app")
	if err != nil {
		t.Fatal(err)
	}
	p.sessions.getOrCreateEndpoints(ended) // as relay traffic would have

	// Block 111: the full node now answers with the next session.
	next := cloneSession(fn.session, "probe-session-2", 111, 120)
	fn.session = next
	p.sessions.latestBlockHeight.Store(111)

	if s, _ := p.sessions.getSession(context.Background(), "eth", "pokt1app"); s.SessionId != "probe-session" {
		t.Fatalf("precondition: HTTP serves the ended session through grace, got %s", s.SessionId)
	}

	addr := domain.EndpointAddr("pokt1supplier-" + wsURL(supplier))
	frame := []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`)
	if got := r.runProbe(context.Background(), wsProbeTarget{serviceID: "eth", addr: addr, url: wsURL(supplier), frame: frame}); got != wsProbeOK {
		t.Fatalf("probe = %s, want ok", got)
	}
	signer.mu.Lock()
	defer signer.mu.Unlock()
	if len(signer.sessions) != 1 || signer.sessions[0] != "probe-session-2" {
		t.Fatalf("probe signed sessions %v; want the session at the current height, probe-session-2", signer.sessions)
	}
}

func cloneSession(s *sessiontypes.Session, id string, start, end int64) *sessiontypes.Session {
	c := *s
	h := *s.Header
	c.SessionId, h.SessionId = id, id
	h.SessionStartBlockHeight, h.SessionEndBlockHeight = start, end
	c.Header = &h
	return &c
}

// A bridge's dial and rebind resolve their endpoint the same way: past the
// end, the target carries the next session, and so does the candidate list.
func TestWSResolveEndpoint_UsesTheSessionAtTheCurrentHeight(t *testing.T) {
	enabled := map[string]bool{featureflag.FlagWebsocketRelays: true, featureflag.FlagWebsocketProbes: true}
	supplier := newEchoSupplier(t)
	r, _, _ := probeFixture(t, wsURL(supplier), 100, `{"jsonrpc":"2.0","id":1,"result":"0x10"}`, enabled)
	p := r.deps.Protocol
	fn := p.fullNode.(*mockRelayFullNode)
	p.sessions.graceBlocks.Store(10)
	p.sessions.latestBlockHeight.Store(105)
	ended, err := p.sessions.getSession(context.Background(), "eth", "pokt1app")
	if err != nil {
		t.Fatal(err)
	}
	p.sessions.getOrCreateEndpoints(ended)

	fn.session = cloneSession(fn.session, "probe-session-2", 111, 120)
	p.sessions.latestBlockHeight.Store(111)

	target, reason, err := r.resolveEndpoint(context.Background(), "eth", map[domain.EndpointAddr]bool{}, nil)
	if err != nil {
		t.Fatalf("resolveEndpoint: %s: %v", reason, err)
	}
	if target.session.SessionId != "probe-session-2" {
		t.Fatalf("dial signs session %s; want the session at the current height, probe-session-2", target.session.SessionId)
	}
}

// A failed fetch past the end leaves the ended session in hand, and within
// grace it is still what a WebSocket signs: the HA miner honours it, and
// refusing the dial would turn a full-node blip into a refused connection.
func TestCurrentSession_FetchErrorKeepsTheEndedSessionThroughGrace(t *testing.T) {
	fn := &stubFullNode{session: buildTestSession("s1", "pokt1supplier", "https://relay.example.com"), height: 105}
	sm := newSessionManager(fn, map[domain.ServiceID]struct{}{"eth": {}}, newTestLogger())
	sm.graceBlocks.Store(10)
	sm.latestBlockHeight.Store(105)
	if _, err := sm.getSession(context.Background(), "eth", "pokt1app"); err != nil {
		t.Fatal(err)
	}
	fn.sessErr = errors.New("full node unavailable")
	sm.latestBlockHeight.Store(111)

	s, err := sm.currentSession(context.Background(), "eth", "pokt1app")
	if err != nil || s.SessionId != "s1" {
		t.Fatalf("currentSession = %v, %v; want the ended session within grace", s, err)
	}
}
