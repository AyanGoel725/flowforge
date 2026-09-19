package retry

import (
	"math"
	"math/rand"
	"time"
)

// Policy defines parameters for exponential backoff retry scheduling.
type Policy struct {
	BaseDelay time.Duration
	MaxDelay  time.Duration
	Jitter    bool
}

// DefaultPolicy provides production-ready retry defaults:
// 1s base delay, 30s maximum delay, with jitter enabled.
func DefaultPolicy() Policy {
	return Policy{
		BaseDelay: 1 * time.Second,
		MaxDelay:  30 * time.Second,
		Jitter:    true,
	}
}

// NextDelay calculates the duration to wait before the next execution attempt.
// Formula: delay = min(base_delay * 2^(attempt - 1), max_delay) (+ optional jitter)
// Jitter decorrelates concurrent retry attempts, preventing thundering herds and synchronized retry storms on dependent services.
func (p Policy) NextDelay(attempt int) time.Duration {
	if attempt <= 0 {
		attempt = 1
	}

	multiplier := math.Pow(2, float64(attempt-1))
	delaySec := p.BaseDelay.Seconds() * multiplier
	maxSec := p.MaxDelay.Seconds()

	if delaySec > maxSec || multiplier > 1e6 {
		delaySec = maxSec
	}

	calculated := time.Duration(delaySec * float64(time.Second))

	if p.Jitter && calculated > 0 {
		// Full/Decorrelated jitter between [calculated * 0.5, calculated]
		half := calculated / 2
		jitterRange := int64(half)
		if jitterRange > 0 {
			jitter := time.Duration(rand.Int63n(jitterRange))
			calculated = half + jitter
		}
	}

	return calculated
}

// NextAttemptTime calculates the timestamp at which the next retry attempt should execute.
func (p Policy) NextAttemptTime(attempt int) time.Time {
	return time.Now().Add(p.NextDelay(attempt))
}
