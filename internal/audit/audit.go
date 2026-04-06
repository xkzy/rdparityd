package audit

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Event struct {
	Timestamp time.Time `json:"timestamp"`
	Operation string    `json:"operation"`
	User      string    `json:"user,omitempty"`
	Result    string    `json:"result"`
	Details   string    `json:"details,omitempty"`
	IPAddress string    `json:"ip_address,omitempty"`
}

type Logger struct {
	mu      sync.Mutex
	file    *os.File
	enabled bool
}

var global = &Logger{
	enabled: false,
}

func Enable(path string) error {
	if path == "" {
		return nil
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return err
	}

	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o640)
	if err != nil {
		return err
	}

	global.mu.Lock()
	global.file = f
	global.enabled = true
	global.mu.Unlock()

	return nil
}

func Disable() {
	global.mu.Lock()
	defer global.mu.Unlock()
	if global.file != nil {
		global.file.Close()
		global.file = nil
	}
	global.enabled = false
}

func Log(op, user, result, details string) {
	global.mu.Lock()
	defer global.mu.Unlock()

	if !global.enabled || global.file == nil {
		return
	}

	event := Event{
		Timestamp: time.Now().UTC(),
		Operation: op,
		User:      user,
		Result:    result,
		Details:   details,
	}

	data, _ := json.Marshal(event)
	global.file.Write(append(data, '\n'))
}

func IsEnabled() bool {
	global.mu.Lock()
	defer global.mu.Unlock()
	return global.enabled
}
