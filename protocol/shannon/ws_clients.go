package shannon

import (
	"sort"
	"sync"
	"time"

	"github.com/pokt-network/sage/domain"
)

// The client ledger answers a question no metric can, because a client
// address cannot be a label: which clients are steering WebSocket traffic,
// and to whom.
//
// A supplier is paid per frame, and a client that opens a chatty subscription
// decides who gets paid for it. A client that reconnects until selection hands
// it a particular operator, then holds that connection for hours, moves
// earnings to that operator that its stake would never have won it. Its trace
// is quick client closes against everyone else and long tenures against one
// owner.
//
// Bounded twice: by clients per window and by suppliers per client. Windowed
// in two buckets so a snapshot covers between one and two windows and nothing
// grows for the life of the process.
const (
	wsLedgerWindow           = time.Hour
	wsLedgerMaxClients       = 4096
	wsLedgerMaxSuppliers     = 32
	wsQuickCloseTenure       = 30 * time.Second
	wsLedgerDefaultSnapshotN = 50

	// A client is flagged as shopping when, over the ledger window, it
	// quick-closed at least wsShoppingMinQuickCloses tenures with owners
	// other than one it spent at least wsShoppingMinShare of its connected
	// time with, and at least wsShoppingMinFavouredTime in all. A script
	// that connects and drops at random has quick closes but no favourite;
	// a long-lived client has a favourite but no quick closes elsewhere.
	wsShoppingMinQuickCloses  = 5
	wsShoppingMinShare        = 0.8
	wsShoppingMinFavouredTime = 10 * time.Minute
)

// wsClientLedger records per-client WebSocket tenures. Safe for concurrent
// use; the zero value is not usable, see newWSClientLedger.
type wsClientLedger struct {
	mu        sync.Mutex
	now       func() time.Time
	started   time.Time // start of cur
	since     time.Time // start of prev, or of cur before the first rotation
	cur, prev map[string]*wsClientStats
	dropped   int // tenures not recorded because the client table was full
}

type wsClientStats struct {
	connections int
	quickCloses int
	suppliers   map[wsSupplierKey]*wsClientSupplier
}

type wsSupplierKey struct {
	service  domain.ServiceID
	operator string
	owner    string
}

type wsClientSupplier struct {
	tenures     int
	seconds     float64
	frames      int64
	quickCloses int
}

func newWSClientLedger(now func() time.Time) *wsClientLedger {
	if now == nil {
		now = time.Now
	}
	t := now()
	return &wsClientLedger{
		now:     now,
		started: t,
		since:   t,
		cur:     make(map[string]*wsClientStats),
		prev:    make(map[string]*wsClientStats),
	}
}

// rotate moves to a fresh window once the current one is full. Caller holds
// mu.
func (l *wsClientLedger) rotate() {
	if now := l.now(); now.Sub(l.started) >= wsLedgerWindow {
		l.prev, l.cur = l.cur, make(map[string]*wsClientStats)
		l.since, l.started = l.started, now
		l.dropped = 0
	}
}

// client returns the current window's entry for ip, or nil when the table is
// full. Caller holds mu.
func (l *wsClientLedger) client(ip string) *wsClientStats {
	l.rotate()
	c := l.cur[ip]
	if c == nil {
		if len(l.cur) >= wsLedgerMaxClients {
			l.dropped++
			return nil
		}
		c = &wsClientStats{suppliers: make(map[wsSupplierKey]*wsClientSupplier)}
		l.cur[ip] = c
	}
	return c
}

// opened counts a connection the client opened.
func (l *wsClientLedger) opened(ip string) {
	if l == nil || ip == "" {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if c := l.client(ip); c != nil {
		c.connections++
	}
}

// tenure records one supplier's time serving the client's connection.
// clientQuit marks a tenure that ended because the client closed.
func (l *wsClientLedger) tenure(ip string, key wsSupplierKey, d time.Duration, frames int64, clientQuit bool) {
	if l == nil || ip == "" {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	c := l.client(ip)
	if c == nil {
		return
	}
	quick := clientQuit && d < wsQuickCloseTenure
	if quick {
		c.quickCloses++
	}
	s := c.suppliers[key]
	if s == nil {
		if len(c.suppliers) >= wsLedgerMaxSuppliers {
			return
		}
		s = &wsClientSupplier{}
		c.suppliers[key] = s
	}
	if quick {
		s.quickCloses++
	}
	s.tenures++
	s.seconds += d.Seconds()
	s.frames += frames
}

// WSClientReport is one client's WebSocket activity over the ledger window.
type WSClientReport struct {
	ClientIP string `json:"client_ip"`
	// Connections is how many connections the client opened.
	Connections int `json:"connections"`
	// QuickCloses is how many supplier tenures the client ended itself
	// within 30s of being bound: a client shopping for a supplier.
	QuickCloses int `json:"quick_client_closes"`
	// Frames is the supplier→client frames across every supplier.
	Frames    int64                  `json:"frames"`
	Suppliers []WSClientSupplierStat `json:"suppliers"`
	// Shopping is set when the client looks like it reconnects until it
	// lands on one owner, then stays: see wsShoppingMinQuickCloses.
	Shopping *WSShopping `json:"shopping,omitempty"`
}

// WSShopping names the owner a shopping client settles on.
type WSShopping struct {
	Owner string `json:"owner"`
	// Share is the fraction of the client's connected time spent with Owner.
	Share float64 `json:"share"`
	// QuickClosesElsewhere counts tenures with other owners the client ended
	// within 30s.
	QuickClosesElsewhere int `json:"quick_closes_elsewhere"`
}

// WSClientSupplierStat is one supplier's share of a client's connections.
type WSClientSupplierStat struct {
	ServiceID     string  `json:"service_id"`
	Operator      string  `json:"operator"`
	Owner         string  `json:"owner"`
	Tenures       int     `json:"tenures"`
	TenureSeconds float64 `json:"tenure_seconds"`
	Frames        int64   `json:"frames"`
	QuickCloses   int     `json:"quick_client_closes"`
}

// WSClientsSnapshot is the ledger as the admin API reports it.
type WSClientsSnapshot struct {
	// WindowSeconds is the span covered: between one and two ledger windows.
	WindowSeconds float64 `json:"window_seconds"`
	// Dropped counts events not recorded because the client table was full.
	Dropped int `json:"dropped"`
	// ShoppingClients counts clients flagged as shopping, before limit.
	ShoppingClients int              `json:"shopping_clients"`
	Clients         []WSClientReport `json:"clients"`
}

// shopping judges one client's merged suppliers; nil when it is not shopping.
func shopping(suppliers []WSClientSupplierStat) *WSShopping {
	byOwner := map[string]float64{}
	var total float64
	for _, s := range suppliers {
		byOwner[s.Owner] += s.TenureSeconds
		total += s.TenureSeconds
	}
	favoured, best := "", 0.0
	for owner, secs := range byOwner {
		if secs > best || (secs == best && owner < favoured) {
			favoured, best = owner, secs
		}
	}
	if total == 0 || best < wsShoppingMinFavouredTime.Seconds() || best/total < wsShoppingMinShare {
		return nil
	}
	elsewhere := 0
	for _, s := range suppliers {
		if s.Owner != favoured {
			elsewhere += s.QuickCloses
		}
	}
	if elsewhere < wsShoppingMinQuickCloses {
		return nil
	}
	return &WSShopping{Owner: favoured, Share: best / total, QuickClosesElsewhere: elsewhere}
}

// ShoppingClients counts the clients currently flagged as shopping, across
// every service. It backs sage_websocket_shopping_clients.
func (l *wsClientLedger) ShoppingClients() int {
	return l.snapshot("", 1, false).ShoppingClients
}

// snapshot merges both windows and returns the top limit clients by frames,
// restricted to serviceID when it is set and to shopping clients when
// onlyShopping is.
func (l *wsClientLedger) snapshot(serviceID domain.ServiceID, limit int, onlyShopping bool) WSClientsSnapshot {
	if limit <= 0 {
		limit = wsLedgerDefaultSnapshotN
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.rotate()

	merged := make(map[string]*WSClientReport)
	bySupplier := make(map[string]map[wsSupplierKey]*WSClientSupplierStat)
	for _, window := range []map[string]*wsClientStats{l.prev, l.cur} {
		for ip, c := range window {
			r := merged[ip]
			if r == nil {
				r = &WSClientReport{ClientIP: ip}
				merged[ip] = r
				bySupplier[ip] = make(map[wsSupplierKey]*WSClientSupplierStat)
			}
			r.Connections += c.connections
			r.QuickCloses += c.quickCloses
			for k, s := range c.suppliers {
				if serviceID != "" && k.service != serviceID {
					continue
				}
				st := bySupplier[ip][k]
				if st == nil {
					st = &WSClientSupplierStat{ServiceID: string(k.service), Operator: k.operator, Owner: k.owner}
					bySupplier[ip][k] = st
				}
				st.Tenures += s.tenures
				st.TenureSeconds += s.seconds
				st.Frames += s.frames
				st.QuickCloses += s.quickCloses
				r.Frames += s.frames
			}
		}
	}

	out := WSClientsSnapshot{
		WindowSeconds: l.now().Sub(l.since).Seconds(),
		Dropped:       l.dropped,
		Clients:       make([]WSClientReport, 0, len(merged)),
	}
	for ip, r := range merged {
		if len(bySupplier[ip]) == 0 && serviceID != "" {
			continue
		}
		r.Suppliers = make([]WSClientSupplierStat, 0, len(bySupplier[ip]))
		for _, st := range bySupplier[ip] {
			r.Suppliers = append(r.Suppliers, *st)
		}
		sort.Slice(r.Suppliers, func(i, j int) bool { return r.Suppliers[i].Frames > r.Suppliers[j].Frames })
		r.Shopping = shopping(r.Suppliers)
		if r.Shopping != nil {
			out.ShoppingClients++
		} else if onlyShopping {
			continue
		}
		out.Clients = append(out.Clients, *r)
	}
	sort.Slice(out.Clients, func(i, j int) bool {
		if out.Clients[i].Frames != out.Clients[j].Frames {
			return out.Clients[i].Frames > out.Clients[j].Frames
		}
		return out.Clients[i].ClientIP < out.Clients[j].ClientIP
	})
	if len(out.Clients) > limit {
		out.Clients = out.Clients[:limit]
	}
	return out
}
