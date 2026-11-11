package main

import (
	"encoding/json"
	"net/http"
	"os"
)

const maxQuoteRequestBytes = 64 << 10

// QuoteRequest is the JSON request payload for POST /api/quote.
type QuoteRequest struct {
	Items  []LineItem `json:"items"`
	Tier   string     `json:"tier"`
	TaxBps int        `json:"taxBps"`
}

// ServiceStatus is returned by GET / and GET /api/info to show the live deployed state.
type ServiceStatus struct {
	Service               string `json:"service"`
	DeployedBy            string `json:"deployedBy"`
	Commit                string `json:"commit"`
	Banner                string `json:"banner"`
	EnterpriseDiscountBps int    `json:"enterpriseDiscountBps"`
	SampleEnterpriseQuote Quote  `json:"sampleEnterpriseQuote"`
}

// NewHandler returns the HTTP handler for fulfillment-api.
// TODO(security): fulfillment-api runs as an internal microservice behind cluster NetworkPolicy /
// API gateway authentication and rate limiting in production deployments.
func NewHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", handleIndex)
	mux.HandleFunc("/api/info", handleIndex)
	mux.HandleFunc("/healthz", handleHealthz)
	mux.HandleFunc("/api/quote", handleQuote)
	return mux
}

func currentServiceStatus() ServiceStatus {
	deployedBy := os.Getenv("DEPLOYED_BY")
	if deployedBy == "" {
		deployedBy = "substrate-actor"
	}
	commit := BuildCommit
	if envCommit := os.Getenv("BUILD_COMMIT"); envCommit != "" && (commit == "" || commit == "dev") {
		commit = envCommit
	}
	sampleQuote, _ := CalculateQuote([]LineItem{
		{SKU: "GKE-SANDBOX-SLOT", Quantity: 4, UnitCents: 2500},
	}, "enterprise", 800)
	return ServiceStatus{
		Service:               "fulfillment-api",
		DeployedBy:            deployedBy,
		Commit:                commit,
		Banner:                PromoBanner,
		EnterpriseDiscountBps: EnterpriseDiscountBps,
		SampleEnterpriseQuote: sampleQuote,
	}
}

func handleIndex(w http.ResponseWriter, r *http.Request) {
	setJSONSecurityHeaders(w)
	if r.URL.Path != "/" && r.URL.Path != "/api/info" {
		writeJSONError(w, http.StatusNotFound, "not found")
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodGet {
		_ = json.NewEncoder(w).Encode(currentServiceStatus())
	}
}

func handleHealthz(w http.ResponseWriter, r *http.Request) {
	setJSONSecurityHeaders(w)
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	st := currentServiceStatus()
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodGet {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status":                "ok",
			"service":               st.Service,
			"deployedBy":            st.DeployedBy,
			"commit":                st.Commit,
			"banner":                st.Banner,
			"enterpriseDiscountBps": st.EnterpriseDiscountBps,
		})
	}
}

func handleQuote(w http.ResponseWriter, r *http.Request) {
	setJSONSecurityHeaders(w)
	if r.Method != http.MethodPost {
		writeJSONError(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxQuoteRequestBytes)
	defer r.Body.Close()

	var req QuoteRequest
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid JSON request payload")
		return
	}

	quote, err := CalculateQuote(req.Items, req.Tier, req.TaxBps)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, err.Error())
		return
	}

	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(quote)
}

func setJSONSecurityHeaders(w http.ResponseWriter) {
	h := w.Header()
	h.Set("Content-Type", "application/json; charset=utf-8")
	h.Set("X-Content-Type-Options", "nosniff")
	h.Set("X-Frame-Options", "DENY")
	h.Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
	h.Set("Permissions-Policy", "camera=(), microphone=(), geolocation=()")
	h.Set("Cache-Control", "no-store")
}

func writeJSONError(w http.ResponseWriter, status int, msg string) {
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}
