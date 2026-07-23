//go:build e2e

package e2e

import (
	"net/http"
	"testing"
	"time"

	vegeta "github.com/tsenart/vegeta/v12/lib"
)

const (
	loadTestRate     = 100 // requests per second
	loadTestDuration = 10 * time.Second
	p99LatencyLimit  = 2 * time.Second
	minSuccessRate   = 0.95
)

var ethBlockNumberBody = []byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`)

// TestLoad_EthBlockNumber runs a 100 rps / 10 s load test against the eth service
// and asserts p99 < 2 s, success rate > 95 %, and zero 5xx responses from SAGE.
func TestLoad_EthBlockNumber(t *testing.T) {
	if !isReachable() {
		t.Skipf("SAGE not reachable at %s", baseURL)
	}

	targeter := vegeta.NewStaticTargeter(vegeta.Target{
		Method: http.MethodPost,
		URL:    baseURL + "/v1",
		Header: http.Header{
			"Target-Service-Id": []string{"eth"},
			"Content-Type":      []string{"application/json"},
		},
		Body: ethBlockNumberBody,
	})

	metrics := runAttack(t, "EthBlockNumber", targeter, loadTestRate, loadTestDuration)

	assertMetrics(t, metrics)
}

// TestLoad_MixedServices sends equal traffic to eth, poly, and solana and
// asserts the same p99 / success-rate / 5xx thresholds.
func TestLoad_MixedServices(t *testing.T) {
	if !isReachable() {
		t.Skipf("SAGE not reachable at %s", baseURL)
	}

	targets := []vegeta.Target{
		{
			Method: http.MethodPost,
			URL:    baseURL + "/v1",
			Header: http.Header{
				"Target-Service-Id": []string{"eth"},
				"Content-Type":      []string{"application/json"},
			},
			Body: ethBlockNumberBody,
		},
		{
			Method: http.MethodPost,
			URL:    baseURL + "/v1",
			Header: http.Header{
				"Target-Service-Id": []string{"poly"},
				"Content-Type":      []string{"application/json"},
			},
			Body: []byte(`{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}`),
		},
		{
			Method: http.MethodPost,
			URL:    baseURL + "/v1",
			Header: http.Header{
				"Target-Service-Id": []string{"solana"},
				"Content-Type":      []string{"application/json"},
			},
			Body: []byte(`{"jsonrpc":"2.0","method":"getSlot","params":[],"id":1}`),
		},
	}

	idx := 0
	targeter := func(tgt *vegeta.Target) error {
		*tgt = targets[idx%len(targets)]
		idx++
		return nil
	}

	metrics := runAttack(t, "MixedServices", targeter, loadTestRate, loadTestDuration)

	assertMetrics(t, metrics)
}

// runAttack fires a vegeta attack and returns the aggregated metrics.
func runAttack(t *testing.T, name string, targeter vegeta.Targeter, rate uint64, dur time.Duration) *vegeta.Metrics {
	t.Helper()

	attacker := vegeta.NewAttacker()
	r := vegeta.Rate{Freq: int(rate), Per: time.Second}

	var metrics vegeta.Metrics
	for res := range attacker.Attack(targeter, r, dur, name) {
		metrics.Add(res)
	}
	metrics.Close()

	t.Logf("[%s] requests=%d success=%.2f%% p99=%s mean=%s",
		name,
		metrics.Requests,
		metrics.Success*100,
		metrics.Latencies.P99,
		metrics.Latencies.Mean,
	)

	return &metrics
}

// assertMetrics checks that p99 latency, success rate, and 5xx counts meet thresholds.
func assertMetrics(t *testing.T, metrics *vegeta.Metrics) {
	t.Helper()

	if metrics.Latencies.P99 > p99LatencyLimit {
		t.Errorf("p99 latency %s exceeds limit %s", metrics.Latencies.P99, p99LatencyLimit)
	}

	if metrics.Success < minSuccessRate {
		t.Errorf("success rate %.2f%% is below %.0f%% threshold", metrics.Success*100, minSuccessRate*100)
	}

	var fivexx int
	for code, count := range metrics.StatusCodes {
		if len(code) == 3 && code[0] == '5' {
			fivexx += count
		}
	}
	if fivexx > 0 {
		t.Errorf("got %d 5xx responses from SAGE (status codes: %v)", fivexx, metrics.StatusCodes)
	}
}

// isReachable returns true if the SAGE base URL responds to a GET /health request.
func isReachable() bool {
	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Get(baseURL + "/health")
	if err != nil {
		return false
	}
	_ = resp.Body.Close()
	return resp.StatusCode == http.StatusOK
}
