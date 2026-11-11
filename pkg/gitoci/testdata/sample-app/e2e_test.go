package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestEndToEndLiveHTTPServer(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen ephemeral port: %v", err)
	}
	addr := ln.Addr().String()

	srv := &http.Server{
		Handler:           NewHandler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		_ = srv.Serve(ln)
	}()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(ctx)
	}()

	baseURL := "http://" + addr

	// 1. E2E check GET / live service status
	resp, err := http.Get(baseURL + "/")
	if err != nil {
		t.Fatalf("E2E GET /: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("E2E GET / status = %d, want 200", resp.StatusCode)
	}
	var status ServiceStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		t.Fatalf("E2E decode ServiceStatus: %v", err)
	}
	if status.Service != "fulfillment-api" || status.Banner == "" {
		t.Fatalf("E2E unexpected ServiceStatus: %+v", status)
	}

	// 2. E2E check POST /api/quote against live TCP listener
	payload, _ := json.Marshal(QuoteRequest{
		Items:  []LineItem{{SKU: "GKE-SANDBOX-SLOT", Quantity: 4, UnitCents: 2500}},
		Tier:   "enterprise",
		TaxBps: 800,
	})
	qResp, err := http.Post(baseURL+"/api/quote", "application/json", bytes.NewReader(payload))
	if err != nil {
		t.Fatalf("E2E POST /api/quote: %v", err)
	}
	defer qResp.Body.Close()
	if qResp.StatusCode != http.StatusOK {
		t.Fatalf("E2E POST /api/quote status = %d, want 200", qResp.StatusCode)
	}
	var quote Quote
	if err := json.NewDecoder(qResp.Body).Decode(&quote); err != nil {
		t.Fatalf("E2E decode Quote: %v", err)
	}
	wantDiscount := (10000 * EnterpriseDiscountBps) / 10000
	if quote.DiscountCents != wantDiscount {
		t.Fatalf("E2E DiscountCents = %d, want %d", quote.DiscountCents, wantDiscount)
	}
}
