package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestServerEndpoints(t *testing.T) {
	srv := httptest.NewServer(NewHandler())
	defer srv.Close()

	// 1. GET /healthz
	healthResp, err := http.Get(srv.URL + "/healthz")
	if err != nil {
		t.Fatalf("GET /healthz: %v", err)
	}
	defer healthResp.Body.Close()
	if healthResp.StatusCode != http.StatusOK {
		t.Fatalf("GET /healthz status = %d, want 200", healthResp.StatusCode)
	}
	if got := healthResp.Header.Get("X-Content-Type-Options"); got != "nosniff" {
		t.Fatalf("X-Content-Type-Options = %q, want nosniff", got)
	}

	// 2. POST /api/quote (standard tier)
	reqBody, err := json.Marshal(QuoteRequest{
		Items:  []LineItem{{SKU: "GKE-NODE-POOL", Quantity: 3, UnitCents: 4000}},
		Tier:   "standard",
		TaxBps: 825,
	})
	if err != nil {
		t.Fatalf("marshal QuoteRequest: %v", err)
	}
	quoteResp, err := http.Post(srv.URL+"/api/quote", "application/json", bytes.NewReader(reqBody))
	if err != nil {
		t.Fatalf("POST /api/quote: %v", err)
	}
	defer quoteResp.Body.Close()
	if quoteResp.StatusCode != http.StatusOK {
		t.Fatalf("POST /api/quote status = %d, want 200", quoteResp.StatusCode)
	}

	var got Quote
	if err := json.NewDecoder(quoteResp.Body).Decode(&got); err != nil {
		t.Fatalf("decode Quote: %v", err)
	}
	if got.SubtotalCents != 12000 || got.TaxCents != 990 || got.TotalCents != 12990 {
		t.Fatalf("unexpected quote: %+v", got)
	}

	// 3. Reject disallowed HTTP methods.
	badMethodResp, err := http.Get(srv.URL + "/api/quote")
	if err != nil {
		t.Fatalf("GET /api/quote: %v", err)
	}
	defer badMethodResp.Body.Close()
	if badMethodResp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET /api/quote status = %d, want 405", badMethodResp.StatusCode)
	}
}
