package metadata

import (
	"os"
	"testing"
)

func FuzzMetadataLoad(f *testing.F) {
	f.Add([]byte("RTPM\x00\x01"))
	f.Add([]byte("XXXX"))
	f.Add([]byte{})

	f.Fuzz(func(t *testing.T, data []byte) {
		path := t.TempDir() + "/metadata.bin"
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Skip()
		}
		store := &Store{path: path}
		_, err := store.Load()
		if err != nil {
			t.Logf("Load rejected data: %v", err)
		}
	})
}
