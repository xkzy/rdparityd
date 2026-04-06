package journal

import (
	"os"
	"testing"
)

func FuzzLoadRebuildProgress(f *testing.F) {
	f.Add([]byte{})
	f.Add([]byte("RBLD"))
	f.Add([]byte("RBLD\x00\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00"))
	f.Add([]byte("XXXX"))

	f.Fuzz(func(t *testing.T, data []byte) {
		path := t.TempDir() + "/rebuild-progress.bin"
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Skip()
		}
		_, err := loadRebuildProgress(path, "disk-a")
		if err != nil {
			t.Logf("loadRebuildProgress rejected: %v", err)
		}
	})
}
