package circuitbreaker

import (
	"errors"
	"sync"
	"time"
)

var ErrOpen = errors.New("circuit breaker is open")

type State int

const (
	StateClosed State = iota
	StateOpen
	StateHalfOpen
)

type Breaker struct {
	mu           sync.Mutex
	failures     int
	threshold    int
	timeout      time.Duration
	state        State
	lastFailure  time.Time
	halfOpenHits int
}

func New(threshold int, timeout time.Duration) *Breaker {
	return &Breaker{
		threshold: threshold,
		timeout:   timeout,
		state:     StateClosed,
	}
}

func (b *Breaker) Execute(fn func() error) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.state == StateOpen {
		if time.Since(b.lastFailure) > b.timeout {
			b.state = StateHalfOpen
			b.halfOpenHits = 0
		} else {
			return ErrOpen
		}
	}

	err := fn()

	if err != nil {
		b.failures++
		b.lastFailure = time.Now()

		if b.failures >= b.threshold {
			b.state = StateOpen
		}

		if b.state == StateHalfOpen {
			b.halfOpenHits++
			if b.halfOpenHits >= 2 {
				b.state = StateOpen
				b.halfOpenHits = 0
			}
		}
	} else {
		if b.state == StateHalfOpen {
			b.reset()
		}
		b.failures = 0
	}

	return err
}

func (b *Breaker) reset() {
	b.state = StateClosed
	b.failures = 0
	b.halfOpenHits = 0
}

func (b *Breaker) State() State {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state
}

func (b *Breaker) Failures() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.failures
}
