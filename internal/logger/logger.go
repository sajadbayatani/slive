package logger

import (
	"log/slog"
	"os"
)

type Logger struct {
	*slog.Logger
}

func New() *Logger {
	return &Logger{
		Logger: slog.New(
			slog.NewJSONHandler(
				os.Stdout,
				&slog.HandlerOptions{
					Level: slog.LevelInfo,
				},
			),
		),
	}
}
