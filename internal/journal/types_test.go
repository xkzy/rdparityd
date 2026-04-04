package journal

import "testing"

func TestValidateSequence(t *testing.T) {
	err := ValidateSequence([]State{
		StatePrepared,
		StateDataWritten,
		StateParityWritten,
		StateMetadataWritten,
		StateCommitted,
	})
	if err != nil {
		t.Fatalf("expected valid sequence, got %v", err)
	}
}

func TestRejectsInvalidSequence(t *testing.T) {
	err := ValidateSequence([]State{
		StatePrepared,
		StateMetadataWritten,
	})
	if err == nil {
		t.Fatal("expected invalid transition error")
	}
}
