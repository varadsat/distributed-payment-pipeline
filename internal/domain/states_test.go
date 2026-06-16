package domain

import (
	"testing"
)

func TestTransition_ValidTransitions(t *testing.T) {
	valid := []struct {
		from, to State
	}{
		{StateReceived, StateValidated},
		{StateReceived, StateFailed},
		{StateValidated, StateAuthorized},
		{StateValidated, StateFailed},
		{StateAuthorized, StateCaptured},
		{StateAuthorized, StateFailed},
		{StateCaptured, StateSettled},
		{StateCaptured, StateFailed},
		{StateSettled, StateReconciled},
	}

	for _, tc := range valid {
		if err := Transition(tc.from, tc.to); err != nil {
			t.Errorf("expected %s -> %s to be valid, got error: %v", tc.from, tc.to, err)
		}
	}
}

func TestTransition_InvalidTransitions(t *testing.T) {
	invalid := []struct {
		from, to State
	}{
		// backwards / skipping
		{StateValidated, StateReceived},
		{StateAuthorized, StateReceived},
		{StateAuthorized, StateValidated},
		{StateCaptured, StateReceived},
		{StateCaptured, StateAuthorized},
		{StateSettled, StateCaptured},
		{StateReconciled, StateSettled},
		// terminal states have no outgoing edges
		{StateFailed, StateReceived},
		{StateFailed, StateValidated},
		{StateReconciled, StateReceived},
		// self-loops
		{StateReceived, StateReceived},
		{StateFailed, StateFailed},
		{StateReconciled, StateReconciled},
		// unknown states
		{State("UNKNOWN"), StateReceived},
		{StateReceived, State("UNKNOWN")},
		// StateSettled cannot go to StateFailed
		{StateSettled, StateFailed},
	}

	for _, tc := range invalid {
		if err := Transition(tc.from, tc.to); err == nil {
			t.Errorf("expected %s -> %s to be invalid, but got nil error", tc.from, tc.to)
		}
	}
}

func TestTransition_ErrorMessage(t *testing.T) {
	err := Transition(StateReceived, StateReconciled)
	if err == nil {
		t.Fatal("expected error for illegal transition, got nil")
	}

	want := "illegal state transition: RECEIVED -> RECONCILED"
	if err.Error() != want {
		t.Errorf("unexpected error message:\n  got:  %q\n  want: %q", err.Error(), want)
	}
}
