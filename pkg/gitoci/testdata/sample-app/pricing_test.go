package main

import "testing"

func TestCalculateTotal(t *testing.T) {
	cases := []struct {
		name          string
		subtotalCents int
		discountCents int
		want          int
	}{
		{
			name:          "zero discount leaves subtotal unchanged",
			subtotalCents: 12000,
			discountCents: 0,
			want:          12000,
		},
		{
			name:          "enterprise 15 percent discount subtracts exact cents",
			subtotalCents: 10000,
			discountCents: 1500,
			want:          8500,
		},
		{
			name:          "partner 10 percent discount subtracts exact cents",
			subtotalCents: 25000,
			discountCents: 2500,
			want:          22500,
		},
		{
			name:          "discount exceeding subtotal floors at zero",
			subtotalCents: 500,
			discountCents: 750,
			want:          0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := CalculateTotal(tc.subtotalCents, tc.discountCents)
			if got != tc.want {
				t.Fatalf("CalculateTotal(%d, %d) = %d, want %d (off-by-one cent in net total calculation)",
					tc.subtotalCents, tc.discountCents, got, tc.want)
			}
		})
	}
}

func TestCalculateQuote(t *testing.T) {
	if EnterpriseDiscountBps < 0 || EnterpriseDiscountBps > 8000 {
		t.Fatalf("EnterpriseDiscountBps = %d out of valid policy range [0, 8000]", EnterpriseDiscountBps)
	}
	items := []LineItem{
		{SKU: "GKE-SANDBOX-SLOT", Quantity: 4, UnitCents: 2500},
	}

	quote, err := CalculateQuote(items, "enterprise", 800)
	if err != nil {
		t.Fatalf("CalculateQuote unexpected error: %v", err)
	}
	if quote.SubtotalCents != 10000 {
		t.Fatalf("SubtotalCents = %d, want 10000", quote.SubtotalCents)
	}
	wantDiscount := (10000 * EnterpriseDiscountBps) / 10000
	if quote.DiscountCents != wantDiscount {
		t.Fatalf("DiscountCents = %d, want %d", quote.DiscountCents, wantDiscount)
	}
	wantNet := 10000 - wantDiscount
	wantTax := (wantNet * 800) / 10000
	if quote.TaxCents != wantTax {
		t.Fatalf("TaxCents = %d, want %d", quote.TaxCents, wantTax)
	}
	if quote.TotalCents != wantNet+wantTax {
		t.Fatalf("TotalCents = %d, want %d", quote.TotalCents, wantNet+wantTax)
	}

	if _, err := CalculateQuote(nil, "standard", 0); err == nil {
		t.Fatal("expected error for empty items, got nil")
	}
	if _, err := CalculateQuote(items, "unknown-tier", 0); err == nil {
		t.Fatal("expected error for unsupported tier, got nil")
	}
}
