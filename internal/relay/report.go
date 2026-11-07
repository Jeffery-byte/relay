package relay

import (
	"errors"
	"math"
)

const MaxNumbers = 10000

type Report struct {
	Count int     `json:"count"`
	Sum   float64 `json:"sum"`
	Min   float64 `json:"min"`
	Max   float64 `json:"max"`
	Mean  float64 `json:"mean"`
}

func Validate(numbers []float64) error {
	if len(numbers) == 0 || len(numbers) > MaxNumbers {
		return errors.New("numbers must contain between 1 and 10000 values")
	}
	for _, n := range numbers {
		if math.IsNaN(n) || math.IsInf(n, 0) {
			return errors.New("numbers must be finite")
		}
	}
	return nil
}

func GenerateReport(numbers []float64) (Report, error) {
	if err := Validate(numbers); err != nil {
		return Report{}, err
	}
	r := Report{Count: len(numbers), Min: numbers[0], Max: numbers[0]}
	for _, n := range numbers {
		r.Sum += n
		r.Min = math.Min(r.Min, n)
		r.Max = math.Max(r.Max, n)
	}
	if math.IsInf(r.Sum, 0) || math.IsNaN(r.Sum) {
		return Report{}, errors.New("report sum exceeds supported numeric range")
	}
	r.Mean = r.Sum / float64(r.Count)
	return r, nil
}
