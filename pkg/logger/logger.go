package logger

import (
	"fmt"
	"log"
	"os"
)

const (
	// ANSI color codes
	colorReset = "\033[0m"
	colorDebug = "\033[36m" // Cyan
	colorInfo  = "\033[32m" // Green
	colorWarn  = "\033[33m" // Yellow
	colorError = "\033[31m" // Red
	colorBold  = "\033[1m"
)

var debugEnabled = os.Getenv("DEBUG") == "true"

// Debug logs debug messages (only if DEBUG=true)
func Debug(format string, v ...interface{}) {
	if debugEnabled {
		log.Printf("%s[DEBUG]%s %s", colorDebug+colorBold, colorReset, fmt.Sprintf(format, v...))
	}
}

// Info logs info messages
func Info(format string, v ...interface{}) {
	log.Printf("%s[INFO]%s %s", colorInfo+colorBold, colorReset, fmt.Sprintf(format, v...))
}

// Warn logs warning messages
func Warn(format string, v ...interface{}) {
	log.Printf("%s[WARN]%s %s", colorWarn+colorBold, colorReset, fmt.Sprintf(format, v...))
}

// Error logs error messages
func Error(format string, v ...interface{}) {
	log.Printf("%s[ERROR]%s %s", colorError+colorBold, colorReset, fmt.Sprintf(format, v...))
}
