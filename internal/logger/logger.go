package logger

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"sync"
	"time"
)

type Level string

const (
	LevelDebug Level = "debug"
	LevelInfo  Level = "info"
	LevelWarn  Level = "warn"
	LevelError Level = "error"
)

type Logger struct {
	mu       sync.Mutex
	output   io.Writer
	minLevel Level
	json     bool
}

var defaultLogger Logger

func init() {
	defaultLogger = Logger{
		output:   os.Stderr,
		minLevel: LevelInfo,
		json:     true,
	}
}

func SetOutput(w io.Writer) {
	defaultLogger.mu.Lock()
	defer defaultLogger.mu.Unlock()
	defaultLogger.output = w
}

func SetMinLevel(level Level) {
	defaultLogger.mu.Lock()
	defer defaultLogger.mu.Unlock()
	defaultLogger.minLevel = level
}

func SetJSON(enabled bool) {
	defaultLogger.mu.Lock()
	defer defaultLogger.mu.Unlock()
	defaultLogger.json = enabled
}

func Debug(msg string, fields ...any) {
	defaultLogger.log(LevelDebug, msg, fields...)
}

func Info(msg string, fields ...any) {
	defaultLogger.log(LevelInfo, msg, fields...)
}

func Warn(msg string, fields ...any) {
	defaultLogger.log(LevelWarn, msg, fields...)
}

func Error(msg string, fields ...any) {
	defaultLogger.log(LevelError, msg, fields...)
}

func (l *Logger) log(level Level, msg string, fields ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()

	if !l.shouldLog(level) {
		return
	}

	entry := map[string]any{
		"timestamp": time.Now().UTC().Format(time.RFC3339Nano),
		"level":     level,
		"message":   msg,
	}

	for i := 0; i < len(fields); i += 2 {
		if i+1 < len(fields) {
			key, ok := fields[i].(string)
			if ok {
				entry[key] = fields[i+1]
			}
		}
	}

	if l.json {
		data, _ := json.Marshal(entry)
		fmt.Fprintln(l.output, string(data))
	} else {
		fmt.Fprintf(l.output, "%s %s %s\n",
			entry["timestamp"],
			level,
			msg,
		)
	}
}

func (l *Logger) shouldLog(level Level) bool {
	levels := map[Level]int{
		LevelDebug: 0,
		LevelInfo:  1,
		LevelWarn:  2,
		LevelError: 3,
	}
	current, ok := levels[level]
	if !ok {
		return false
	}
	min, ok := levels[l.minLevel]
	return ok && current >= min
}

type Loggable interface {
	LogFields() map[string]any
}

func InfoLoggable(l Loggable) {
	if l != nil {
		Info(l.LogFields()["message"].(string), l.LogFields())
	}
}
