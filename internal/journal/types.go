package journal

import "fmt"

type State string

const (
	StatePrepared        State = "prepared"
	StateDataWritten     State = "data-written"
	StateParityWritten   State = "parity-written"
	StateMetadataWritten State = "metadata-written"
	StateCommitted       State = "committed"
	StateAborted         State = "aborted"
	StateReplayRequired  State = "replay-required"
)

var allowedTransitions = map[State][]State{
	StatePrepared:        {StateDataWritten, StateAborted, StateReplayRequired},
	StateDataWritten:     {StateParityWritten, StateAborted, StateReplayRequired},
	StateParityWritten:   {StateMetadataWritten, StateReplayRequired},
	StateMetadataWritten: {StateCommitted, StateReplayRequired},
	StateReplayRequired:  {StateCommitted, StateAborted},
}

func CanTransition(from, to State) bool {
	for _, allowed := range allowedTransitions[from] {
		if allowed == to {
			return true
		}
	}
	return false
}

func ValidateTransition(from, to State) error {
	if CanTransition(from, to) {
		return nil
	}
	return fmt.Errorf("invalid journal transition %q -> %q", from, to)
}

func ValidateSequence(states []State) error {
	if len(states) == 0 {
		return fmt.Errorf("empty state sequence")
	}

	for i := 1; i < len(states); i++ {
		if err := ValidateTransition(states[i-1], states[i]); err != nil {
			return err
		}
	}
	return nil
}

func ValidateRecordSequence(records []Record) error {
	if len(records) == 0 {
		return fmt.Errorf("empty state sequence")
	}
	states := make([]State, 0, len(records))
	for _, record := range records {
		states = append(states, record.State)
	}
	if !isRepairRecord(records[0]) {
		return ValidateSequence(states)
	}
	for i := 1; i < len(states); i++ {
		from := states[i-1]
		to := states[i]
		if from == StatePrepared && (to == StateDataWritten || to == StateReplayRequired) {
			continue
		}
		if from == StateDataWritten && (to == StateCommitted || to == StateReplayRequired) {
			continue
		}
		if from == StateReplayRequired && (to == StateCommitted || to == StateAborted) {
			continue
		}
		return fmt.Errorf("invalid journal transition %q -> %q", from, to)
	}
	return nil
}
