package domain

import (
	"fmt"
	"slices"
)

// State is the payment lifecycle state machine.
type State string

const (
	StateReceived   State = "RECEIVED"
	StateValidated  State = "VALIDATED"
	StateAuthorized State = "AUTHORIZED"
	StateCaptured   State = "CAPTURED"
	StateSettled    State = "SETTLED"
	StateReconciled State = "RECONCILED"
	StateFailed     State = "FAILED"
)

// allowed encodes the legal transitions. Any move not listed is rejected.
var allowed = map[State][]State{
	StateReceived:   {StateValidated, StateFailed},
	StateValidated:  {StateAuthorized, StateCaptured, StateFailed},
	StateAuthorized: {StateCaptured, StateFailed},
	StateCaptured:   {StateSettled, StateFailed},
	StateSettled:    {StateReconciled},
}

// Transition validates and returns the next state, or an error if illegal.
func Transition(from, to State) error {
	if slices.Contains(allowed[from], to) {
		return nil
	}
	return fmt.Errorf("illegal state transition: %s -> %s", from, to)
}
