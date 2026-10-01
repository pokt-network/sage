package evm

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/tidwall/gjson"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/qos"
)

// The state canary is an eth_call at "latest" whose answer carries the
// timestamp of the block it was computed at, sent byte-identical every time
// (featureflag.FlagStateCanary).
//
// It exists for a response cache keyed on the request body. On mainnet
// (2026-10-01) one owner's cache answered a fixed Multicall3 call with the
// same block for over two minutes on bsc (~320 blocks) while the same call
// with a fresh id was current. Head calls show such a cache only for head
// methods; most of what dapps poll is identical eth_calls for state, which
// the cache serves just as old and nothing else measures.
//
// Every body calls Multicall3, at the address it has on nearly every EVM
// chain, and ends with getCurrentBlockTimestamp, so every answer can be
// graded the same way: block.timestamp, not block.number, which inside
// eth_call is Ethereum's on Arbitrum. A chain without Multicall3 there
// answers with nothing to decode and is not graded. Four shapes, rotated
// every canaryRotation, so the cache cannot be bypassed for one body only;
// each is byte-identical while it is in use, which is what lets a cache
// serve it old. Each makes one call: some gateways refuse a multicall of
// more than one ("request is too complex", -32602, seen 2026-10-01).
const (
	multicall3 = "ca11bde05977b3631167028862be2a173976ca11"

	selGetCurrentBlockTimestamp = "0f28c97d"
	selAggregate3               = "82ad56cb"
	selTryAggregate             = "bce38bd7"

	// canaryRotation is how long one body is used before the next.
	canaryRotation = 10 * time.Minute

	// CanaryName is the health check's name, and the method label its
	// answers are counted under.
	CanaryName = "eth_call_canary"
)

// canaryBodies are the request bodies, built once.
var canaryBodies = func() [][]byte {
	word := func(n int) string { return fmt.Sprintf("%064x", n) }
	addr := strings.Repeat("0", 24) + multicall3
	pad := func(data string) string { return data + strings.Repeat("0", (64-len(data)%64)%64) }
	bytesArg := func(data string) string { return word(len(data)/2) + pad(data) }
	// aggregate3((address target, bool allowFailure, bytes callData)[]) with
	// one call, and tryAggregate(bool requireSuccess, (address, bytes)[]).
	aggregate3 := func(allowFailure int) string {
		tuple := addr + word(allowFailure) + word(0x60) + bytesArg(selGetCurrentBlockTimestamp)
		return selAggregate3 + word(0x20) + word(1) + word(0x20) + tuple
	}
	tryAggregate := func() string {
		tuple := addr + word(0x40) + bytesArg(selGetCurrentBlockTimestamp)
		return selTryAggregate + word(0) + word(0x40) + word(1) + word(0x20) + tuple
	}
	data := []string{
		selGetCurrentBlockTimestamp,
		aggregate3(1),
		aggregate3(0),
		tryAggregate(),
	}
	out := make([][]byte, len(data))
	for i, d := range data {
		out[i] = []byte(fmt.Sprintf(`{"jsonrpc":"2.0","method":"eth_call","params":[{"to":"0x%s","data":"0x%s"},"latest"],"id":%d}`, multicall3, d, 11+i))
	}
	return out
}()

// CanaryCheck is the state canary's health check for the body in use at now,
// for every plugin that serves an EVM face.
func CanaryCheck(now time.Time) qos.HealthCheck {
	body := canaryBodies[(now.Unix()/int64(canaryRotation/time.Second))%int64(len(canaryBodies))]
	return qos.HealthCheck{
		Name:       CanaryName,
		Payload:    domain.NewPayload(body, domain.RPCTypeJSONRPC, "eth_call"),
		GradesHead: true,
	}
}

// isCanary reports whether a request is one of the canary bodies.
func isCanary(request []byte) bool {
	for _, b := range canaryBodies {
		if bytes.Equal(request, b) {
			return true
		}
	}
	return false
}

// canaryTimestamp reads the block timestamp a canary answer carries: the
// whole result for the direct call, the last call's return data for
// aggregate3 and tryAggregate (both answer Result[]). ok is false for anything else: an error, a revert, an empty
// result from a chain without Multicall3, or a last call that failed.
func canaryTimestamp(response []byte) (time.Time, bool) {
	raw, err := hex.DecodeString(strings.TrimPrefix(gjson.GetBytes(response, "result").String(), "0x"))
	if err != nil || len(raw) == 0 || len(raw)%32 != 0 {
		return time.Time{}, false
	}
	// word reads the 32-byte word at off as an offset, length or flag: a
	// small number, or not ok when it is out of range of the result.
	word := func(off int) (int, bool) {
		if off < 0 || off+32 > len(raw) {
			return 0, false
		}
		v := new(big.Int).SetBytes(raw[off : off+32])
		if !v.IsInt64() || v.Int64() > int64(len(raw)) {
			return 0, false
		}
		return int(v.Int64()), true
	}
	value := raw
	if len(raw) > 32 {
		// Result[]: offset to the array, its length, one offset per tuple
		// (from the start of those offsets), each tuple (success, offset to
		// returnData, then returnData as length + bytes).
		arr, ok1 := word(0)
		n, ok2 := word(arr)
		if !ok1 || !ok2 || n < 1 {
			return time.Time{}, false
		}
		heads := arr + 32
		rel, ok3 := word(heads + 32*(n-1))
		tuple := heads + rel
		success, ok4 := word(tuple)
		dataRel, ok5 := word(tuple + 32)
		size, ok6 := word(tuple + dataRel)
		start := tuple + dataRel + 32
		if !ok3 || !ok4 || !ok5 || !ok6 || success != 1 || size != 32 || start+32 > len(raw) {
			return time.Time{}, false
		}
		value = raw[start : start+32]
	}
	ts := new(big.Int).SetBytes(value)
	if !ts.IsInt64() || ts.Int64() <= 0 {
		return time.Time{}, false
	}
	return time.Unix(ts.Int64(), 0), true
}
