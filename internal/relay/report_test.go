package relay

import (
	"math"
	"testing"
)

func TestReport(t *testing.T) {
	for _, tc := range []struct {
		name    string
		numbers []float64
		want    Report
	}{
		{"positive", []float64{10, 20, 30, 40, 50}, Report{5, 150, 10, 50, 30}},
		{"negative", []float64{-9, -3, -6}, Report{3, -18, -9, -3, -6}},
		{"fractional", []float64{0.5, -0.5, 0}, Report{3, 0, -0.5, 0.5, 0}},
		{"single", []float64{7}, Report{1, 7, 7, 7, 7}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := GenerateReport(tc.numbers)
			if err != nil || got != tc.want {
				t.Fatalf("got %+v, %v; want %+v", got, err, tc.want)
			}
		})
	}
}

func TestInvalidReport(t *testing.T) {
	for _, input := range [][]float64{nil, {}, make([]float64, MaxNumbers+1), {math.NaN()}, {math.Inf(1)}, {math.MaxFloat64, math.MaxFloat64}} {
		if _, err := GenerateReport(input); err == nil {
			t.Fatal("expected invalid input or overflow to fail")
		}
	}
}
