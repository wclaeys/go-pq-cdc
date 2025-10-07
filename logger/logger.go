package logger

import (
	"context"
	"log/slog"
	"os"
	"sync"
)

var (
	_default     Logger
	once         sync.Once
	debugEnabled bool // set once in InitLogger, then read-only
)

type Logger interface {
	Debug(msg string, args ...any)
	Info(msg string, args ...any)
	Warn(msg string, args ...any)
	Error(msg string, args ...any)
}

func InitLogger(l Logger) {
	once.Do(func() {
		_default = l

		// Precompute and cache "is debug enabled?" once at init.
		type leveled interface {
			Enabled(ctx context.Context, level slog.Level) bool
		}
		if ll, ok := any(l).(leveled); ok {
			debugEnabled = ll.Enabled(context.Background(), slog.LevelDebug)
		} else {
			debugEnabled = false
		}
	})
}

func Debug(msg string, args ...any) {
	_default.Debug(msg, args...)
}

func Info(msg string, args ...any) {
	_default.Info(msg, args...)
}

func Warn(msg string, args ...any) {
	_default.Warn(msg, args...)
}

func Error(msg string, args ...any) {
	_default.Error(msg, args...)
}

func IsDebugEnabled() bool {
	return debugEnabled
}

func NewSlog(logLevel slog.Level) Logger {
	return slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel}))
}
