package journal

import (
	"os"
	"testing"
)

func FuzzLoadScrubProgress(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte("SCRB"))
	f.Add([]byte("SCRB\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"))
	f.Add([]byte("XXXX"))

	f.Fuzz(func(t *testing.T, data []byte) {
		path := t.TempDir() + "/scrub-progress.bin"
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Skip()
		}
		_, err := loadScrubProgress(path)
		if err != nil {
			t.Logf("loadScrubProgress rejected: %v", err)
		}
	})
}
