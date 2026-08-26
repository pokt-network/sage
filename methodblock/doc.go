// Package methodblock remembers that a host could not answer a method
// recently, so selection can route that method elsewhere while the host
// keeps serving everything else.
//
// A mark is host × method × expiry. Marks come from the method_blocks
// middleware after an attempt whose verdict was MethodBlocking (a timeout
// after connect, or an endpoint saying it does not serve the method). A host
// that accumulates escalation-many distinct SUPPLIER-ATTRIBUTED method marks
// inside one TTL is not slow on something, it is dead: it is blocked for every
// method for one TTL, and the method marks are folded into that.
//
// Escalation counts supplier-attributed marks only. A client-attributed mark
// — a -32601 from a healthy node that simply does not implement debug_* —
// keeps that one method away from the host and nothing more; otherwise any
// client could remove a good host from every method by asking it for three
// methods it never claimed to serve.
//
// Local memory only. One mark is one relay_timeout of evidence, not worth a
// Redis round trip, and the gateway must run without Redis anyway.
package methodblock
