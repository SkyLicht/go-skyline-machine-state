package handlers

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// -----------------------------
// Logging: Per-PLC Logger with Rotation
// -----------------------------

// EnsureLogDir makes sure the folder "log/plc" exists.
// If the folder does not exist, it is created.
func EnsureLogDir() {
	log.Printf("EnsureLogDir")
	dir := filepath.Join("log", "plc")
	if err := os.MkdirAll(dir, 0755); err != nil {
		log.Fatalf("Failed to create log directory: %v", err)
	}
}

// PLCLogger holds a logger for a specific PLC along with a counter.
type PLCLogger struct {
	logger   *log.Logger
	file     *os.File
	count    int
	maxLines int
}

var (
	plcLoggers      = make(map[string]*PLCLogger)
	plcLoggersMutex sync.Mutex
	maxLogLines     = 50000
)

// getPLCLogger returns (or creates) a logger for the given PLC ID.
func getPLCLogger(plcID string) *PLCLogger {
	plcLoggersMutex.Lock()
	defer plcLoggersMutex.Unlock()
	if logger, exists := plcLoggers[plcID]; exists {
		return logger
	}
	filePath := filepath.Join("log", "plc", fmt.Sprintf("plc_%s.log", plcID))
	// Open the file with O_CREATE so that if the file does not exist, it is created.
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		log.Printf("Error opening log file for PLC %s: %v", plcID, err)
		return &PLCLogger{logger: log.Default(), count: 0, maxLines: maxLogLines}
	}
	plcLogger := &PLCLogger{
		logger:   log.New(file, "", log.LstdFlags),
		file:     file,
		count:    0,
		maxLines: maxLogLines,
	}
	plcLoggers[plcID] = plcLogger
	return plcLogger
}

// Printf logs a message and rotates the log file if the maximum line count is reached.
func (pl *PLCLogger) Printf(format string, v ...interface{}) {
	pl.logger.Printf(format, v...)
	pl.count++
	if pl.count >= pl.maxLines {
		pl.rotate()
	}
}

// rotate closes the current log file, renames it with a timestamp, and creates a new log file.
func (pl *PLCLogger) rotate() {
	if err := pl.file.Close(); err != nil {
		log.Printf("Error closing log file %s: %v", pl.file.Name(), err)
		return
	}
	timestamp := time.Now().Format("20060102_150405")
	oldName := pl.file.Name()
	newName := fmt.Sprintf("%s_%s", oldName, timestamp)
	if err := os.Rename(oldName, newName); err != nil {
		log.Printf("Error renaming log file %s to %s: %v", oldName, newName, err)
		return
	}
	newFile, err := os.OpenFile(oldName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		pl.logger = log.Default()
	} else {
		pl.file = newFile
		pl.logger = log.New(newFile, "", log.LstdFlags)
	}
	pl.count = 0
}
