package middleware

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/pokt-network/sage/domain"
	"github.com/pokt-network/sage/relay"
)

// gatedWriter is an http.ResponseWriter a test can slow down or break: writes
// after the first `free` block until release is closed, and the write numbered
// failAt (1-based) and every later one fail, as a closed connection does.
type gatedWriter struct {
	*httptest.ResponseRecorder
	free    int
	release chan struct{}
	failAt  int

	mu     sync.Mutex
	writes int
}

func (g *gatedWriter) Write(p []byte) (int, error) {
	g.mu.Lock()
	g.writes++
	n := g.writes
	g.mu.Unlock()
	if g.failAt > 0 && n >= g.failAt {
		return 0, errors.New("write: broken pipe")
	}
	if g.release != nil && n > g.free {
		<-g.release
	}
	return g.ResponseRecorder.Write(p)
}

// indexedPayloads builds n JSON-RPC payloads whose id is their index.
func indexedPayloads(n int) []domain.Payload {
	out := make([]domain.Payload, n)
	for i := range out {
		body := fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":"eth_getLogs","params":[]}`, i)
		out[i] = domain.NewPayload([]byte(body), domain.RPCTypeJSONRPC, "eth_getLogs")
	}
	return out
}

// answerByID is an inner handler answering payload id with body(id) after
// delay(id), giving up if the sub-relay's context ends first.
func answerByID(body func(id int) string, delay func(id int) time.Duration, calls *atomic.Int32) relay.Handler {
	return relay.HandlerFunc(func(ctx *relay.Context) error {
		calls.Add(1)
		var id int
		_ = json.Unmarshal(ctx.Payloads[0].JSONRPCID(), &id)
		select {
		case <-time.After(delay(id)):
		case <-ctx.Ctx.Done():
			return ctx.Ctx.Err()
		}
		ctx.Response = &domain.Response{Body: []byte(body(id)), HTTPStatusCode: 200}
		return nil
	})
}

func streamLimits(perBatch, window int) BatchLimits {
	return func() (int, int, int, int) { return 10000, 10000, perBatch, window }
}

func streamedCtx(parent context.Context, payloads []domain.Payload, w http.ResponseWriter) *relay.Context {
	req, _ := http.NewRequest(http.MethodPost, "/v1", nil)
	ctx := relay.NewContext(parent, req, nil, relay.NewHTTPResponseWriter(w))
	ctx.ServiceID = "eth"
	ctx.Payloads = payloads
	return ctx
}

func noDelay(int) time.Duration { return 0 }

// The streamed body is byte for byte the body the unstreamed merge builds from
// the same answers — compaction, HTML escaping, order — and it is labelled
// JSON.
func TestBatchStream_ByteIdenticalToMerge(t *testing.T) {
	bodies := []string{
		`{"jsonrpc":"2.0","id":0,"result":"0x1"}`,
		"{\n  \"jsonrpc\": \"2.0\",\n  \"id\": 1,\n  \"result\": [1, 2,  3]\n}\n",
		`{"jsonrpc":"2.0","id":2,"result":"<script>&amp;</script>"}`,
		"{\"jsonrpc\":\"2.0\",\"id\":3,\"result\":\"sep\\u2028   café\"}",
		`{"jsonrpc":"2.0","id":4,"result":12345678901234567890}`,
	}
	var calls atomic.Int32
	inner := answerByID(func(id int) string { return bodies[id] }, noDelay, &calls)

	rec := httptest.NewRecorder()
	streamed := streamedCtx(t.Context(), indexedPayloads(len(bodies)), rec)
	require.NoError(t, Batch(streamLimits(2, 1), nil, nil, nil)(inner).HandleRelay(streamed))

	merged := makeMultiPayloadCtx(indexedPayloads(len(bodies)))
	require.NoError(t, Batch(streamLimits(2, 1), nil, nil, nil)(inner).HandleRelay(merged))

	assert.Equal(t, string(merged.Response.Body), rec.Body.String())
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
	assert.Empty(t, streamed.Response.Body, "the body went to the writer, not to the router")
}

// A body that is not JSON cannot fall back to the whole-response error the
// merge uses — earlier answers are on the wire — so that item alone becomes an
// error carrying its id, and the array stays valid.
func TestBatchStream_NonJSONItemBecomesAnError(t *testing.T) {
	var calls atomic.Int32
	inner := answerByID(func(id int) string {
		if id == 1 {
			return "<html>502 Bad Gateway</html>"
		}
		return fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"result":"ok"}`, id)
	}, noDelay, &calls)
	rec := httptest.NewRecorder()
	require.NoError(t, Batch(streamLimits(2, 1), nil, nil, nil)(inner).HandleRelay(streamedCtx(t.Context(), indexedPayloads(3), rec)))

	var items []map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &items))
	require.Len(t, items, 3)
	assert.EqualValues(t, 1, items[1]["id"])
	assert.Contains(t, fmt.Sprint(items[1]["error"]), "not JSON")
}

// Answers that finish in reverse order are still written in request order.
func TestBatchStream_OutOfOrderCompletion(t *testing.T) {
	const n = 8
	var calls atomic.Int32
	inner := answerByID(func(id int) string { return fmt.Sprintf(`{"id":%d}`, id) },
		func(id int) time.Duration { return time.Duration(n-id) * 5 * time.Millisecond }, &calls)
	rec := httptest.NewRecorder()
	require.NoError(t, Batch(streamLimits(n, n), nil, nil, nil)(inner).HandleRelay(streamedCtx(t.Context(), indexedPayloads(n), rec)))

	var items []struct{ ID int }
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &items))
	require.Len(t, items, n)
	for i, it := range items {
		assert.Equal(t, i, it.ID)
	}
}

// A client that stops reading holds the window: no more than
// max_batch_concurrency + max_batch_window answers are started and unwritten,
// so the held bytes stop growing, and everything is delivered once it reads.
func TestBatchStream_SlowClientHoldsTheWindow(t *testing.T) {
	const n, perBatch, window, size = 40, 2, 3, 100
	body := func(id int) string {
		return fmt.Sprintf(`{"id":%d,"pad":"%s"}`, id, strings.Repeat("x", size-len(fmt.Sprintf(`{"id":%d,"pad":""}`, id))))
	}
	var calls atomic.Int32
	inner := answerByID(body, noDelay, &calls)
	gw := &gatedWriter{ResponseRecorder: httptest.NewRecorder(), free: 2, release: make(chan struct{})}
	g := &batchGauges{}

	done := make(chan error, 1)
	go func() {
		done <- Batch(streamLimits(perBatch, window), nil, nil, g)(inner).HandleRelay(streamedCtx(context.Background(), indexedPayloads(n), gw))
	}()
	time.Sleep(150 * time.Millisecond) // the client is not reading

	assert.LessOrEqual(t, int(calls.Load()), perBatch+window+1, "started while the client was stuck")
	assert.LessOrEqual(t, g.peakBytes.Load(), int64((perBatch+window)*size), "held bytes while the client was stuck")

	close(gw.release)
	require.NoError(t, <-done)
	var items []json.RawMessage
	require.NoError(t, json.Unmarshal(gw.Body.Bytes(), &items))
	assert.Len(t, items, n)
	assert.Zero(t, g.bytes.Load(), "everything held was released")
}

// A client gone mid-stream stops the batch: nothing more is written, payloads
// not yet started never start, what was held is released, and it is counted.
func TestBatchStream_ClientDisconnect(t *testing.T) {
	for name, cut := range map[string]func(cancel context.CancelFunc) *gatedWriter{
		"write fails": func(context.CancelFunc) *gatedWriter {
			return &gatedWriter{ResponseRecorder: httptest.NewRecorder(), failAt: 4}
		},
		"request context cancelled": func(cancel context.CancelFunc) *gatedWriter {
			time.AfterFunc(30*time.Millisecond, cancel)
			return &gatedWriter{ResponseRecorder: httptest.NewRecorder()}
		},
	} {
		t.Run(name, func(t *testing.T) {
			const n = 50
			var calls atomic.Int32
			inner := answerByID(func(id int) string { return fmt.Sprintf(`{"id":%d}`, id) },
				func(int) time.Duration { return 10 * time.Millisecond }, &calls)
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			gw := cut(cancel)
			g := &batchGauges{}

			began := time.Now()
			require.NoError(t, Batch(streamLimits(2, 2), nil, nil, g)(inner).HandleRelay(streamedCtx(parent, indexedPayloads(n), gw)))
			assert.Less(t, time.Since(began), time.Second, "the batch stopped rather than fetching every answer")
			assert.Less(t, int(calls.Load()), n, "payloads after the disconnect were not started")
			assert.Equal(t, int64(1), g.disconnects.Load())
			assert.Zero(t, g.bytes.Load(), "held answers were released")
			assert.Zero(t, g.subRelays.Load())
		})
	}
}

// The batch deadline mid-stream: answers not in get the per-item error, and
// the array still closes so the client can parse what it got.
func TestBatchStream_DeadlineMidStream(t *testing.T) {
	const n = 20
	var calls atomic.Int32
	inner := answerByID(func(id int) string { return fmt.Sprintf(`{"id":%d}`, id) },
		func(id int) time.Duration {
			if id < 4 {
				return 0
			}
			return time.Hour // only the deadline ends these
		}, &calls)
	parent, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	rec := httptest.NewRecorder()
	g := &batchGauges{}
	require.NoError(t, Batch(streamLimits(2, 2), nil, nil, g)(inner).HandleRelay(streamedCtx(parent, indexedPayloads(n), rec)))

	var items []map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &items), "the array must close validly: %s", rec.Body.String())
	require.Len(t, items, n)
	for i := 0; i < 4; i++ {
		assert.EqualValues(t, i, items[i]["id"])
		assert.Nil(t, items[i]["error"])
	}
	for i := 4; i < n; i++ {
		assert.NotNil(t, items[i]["error"], "item %d", i)
	}
	assert.Zero(t, g.disconnects.Load(), "a deadline is not a disconnect")
	assert.Zero(t, g.bytes.Load())
}

// However large the batch, it holds at most (concurrency + window) answers.
func TestBatchStream_LargeBatchHeldBytesBounded(t *testing.T) {
	const n, perBatch, window, size = 500, 4, 4, 1024
	body := func(id int) string {
		prefix := fmt.Sprintf(`{"id":%d,"pad":"`, id)
		return prefix + strings.Repeat("x", size-len(prefix)-2) + `"}`
	}
	var calls atomic.Int32
	// Every 50th answer is slow, so answers after it finish first and would
	// pile up behind it without the window.
	inner := answerByID(body, func(id int) time.Duration {
		if id%50 == 0 {
			return 30 * time.Millisecond
		}
		return 0
	}, &calls)
	rec := httptest.NewRecorder()
	g := &batchGauges{}
	require.NoError(t, Batch(streamLimits(perBatch, window), nil, nil, g)(inner).HandleRelay(streamedCtx(context.Background(), indexedPayloads(n), rec)))

	assert.LessOrEqual(t, g.peakBytes.Load(), int64((perBatch+window)*size))
	var items []json.RawMessage
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &items))
	assert.Len(t, items, n)
}
