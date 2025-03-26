package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/goburrow/modbus"
	"log"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// -----------------------------
// Logging: Per-PLC Logger with Rotation
// -----------------------------

// EnsureLogDir makes sure the folder "log/plc" exists.
// If the folder does not exist, it creates it.
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
// It uses os.OpenFile with the O_CREATE flag so that if the file doesn't exist, it is created.
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

// Printf logs a message and rotates the log file if the max line count is reached.
func (pl *PLCLogger) Printf(format string, v ...interface{}) {
	pl.logger.Printf(format, v...)
	pl.count++
	if pl.count >= pl.maxLines {
		pl.rotate()
	}
}

// rotate closes the current log file, renames it with a timestamp, and creates a new log file.
func (pl *PLCLogger) rotate() {
	err := pl.file.Close()
	if err != nil {
		return
	}
	timestamp := time.Now().Format("20060102_150405")
	oldName := pl.file.Name()
	newName := fmt.Sprintf("%s_%s", oldName, timestamp)
	err = os.Rename(oldName, newName)
	if err != nil {
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

// -----------------------------
// Data Types and Global State
// -----------------------------
// Global states maps.
var (
	plcCoilState     = make(map[string]map[int]bool)
	plcInputState    = make(map[string]map[int]bool)
	plcRegisterState = make(map[string]map[int]uint16)
	coilMetrics      = make(map[string]map[int]*CoilMetrics)
	stateMutex       sync.RWMutex
	EventChan        = make(chan Event, 1000)
	TowerEventChan   = make(chan TowerEvent, 200)
)

// Event represents a change in a coil or holding register value.
type Event struct {
	PLCID     string      `json:"plc_id"`
	Type      string      `json:"type"` // "coil" or "input" or "holding "
	Name      string      `json:"name"`
	Address   int         `json:"address"`
	OldValue  interface{} `json:"old_value"`
	NewValue  interface{} `json:"new_value"`
	Timestamp time.Time   `json:"timestamp"`
}
type TowerEvent struct {
	PLCID     string    `json:"plc_id"`
	Type      string    `json:"type"`
	Yellow    bool      `json:"yellow"`
	Green     bool      `json:"green"`
	Red       bool      `json:"red"`
	Buzzer    bool      `json:"buzzer"`
	Timestamp time.Time `json:"timestamp"`
}

type Register struct {
	Name        string `json:"name"`
	Address     int    `json:"address"`
	Description string `json:"desc"`
}

type PLC struct {
	ID               string     `json:"id"`
	IP               string     `json:"ip"`
	Port             int        `json:"port"`
	Tower            []string   `json:"tower"`
	Coils            []Register `json:"coils"`
	Inputs           []Register `json:"inputs"`
	HoldingRegisters []Register `json:"holding_registers"`
}

// CoilMetrics holds extra measurement data for a coil.
type CoilMetrics struct {
	LastChange      time.Time     `json:"last_change"`
	ActivationCount int           `json:"activation_count"`
	TotalActiveTime time.Duration `json:"total_active_time"`
}

// PLCManager manages active pollers.
type PLCManager struct {
	pollers map[string]context.CancelFunc
	configs map[string]PLC
	lock    sync.Mutex
}

func NewPLCManager() *PLCManager {
	return &PLCManager{
		pollers: make(map[string]context.CancelFunc),
		configs: make(map[string]PLC),
	}
}

func plcEqual(a, b PLC) bool {
	if a.ID != b.ID || a.IP != b.IP || a.Port != b.Port {
		return false
	}
	if len(a.Coils) != len(b.Coils) {
		return false
	}
	for i, coil := range a.Coils {
		if coil != b.Coils[i] {
			return false
		}
	}
	if len(a.HoldingRegisters) != len(b.HoldingRegisters) {
		return false
	}
	for i, reg := range a.HoldingRegisters {
		if reg != b.HoldingRegisters[i] {
			return false
		}
	}
	return true
}

func readInputWithRetry(ctx context.Context, client modbus.Client, plc PLC, input Register) (bool, error) {
	plcLogger := getPLCLogger(plc.ID)
	maxRetries := 3
	for attempt := 0; attempt < maxRetries; attempt++ {
		results, err := client.ReadDiscreteInputs(uint16(input.Address), 1)
		if err != nil {
			waitTime := time.Duration(math.Pow(2, float64(attempt))) * time.Second
			plcLogger.Printf("Error reading input %d (attempt %d): %v. Retrying in %v", input.Address, attempt, err, waitTime)
			select {
			case <-time.After(waitTime):
			case <-ctx.Done():
				return false, fmt.Errorf("context canceled")
			}
			continue
		}
		return results[0] == 1, nil
	}

	return false, fmt.Errorf("failed to read input %d after %d retries", input.Address, maxRetries)
}

func readCoilWithRetry(ctx context.Context, client modbus.Client, plc PLC, coil Register) (bool, error) {
	plcLogger := getPLCLogger(plc.ID)
	maxRetries := 3
	for attempt := 0; attempt <= maxRetries; attempt++ {
		results, err := client.ReadCoils(uint16(coil.Address), 1)
		if err != nil {
			waitTime := time.Duration(math.Pow(2, float64(attempt))) * time.Second
			plcLogger.Printf("Error reading coil %d (attempt %d): %v. Retrying in %v", coil.Address, attempt, err, waitTime)
			select {
			case <-time.After(waitTime):
			case <-ctx.Done():
				return false, fmt.Errorf("context canceled")
			}
			continue
		}
		return results[0] == 1, nil
	}
	return false, fmt.Errorf("failed to read coil %d after %d retries", coil.Address, maxRetries)
}

func readHoldingRegisterWithRetry(ctx context.Context, client modbus.Client, plc PLC, reg Register) (uint16, error) {
	plcLogger := getPLCLogger(plc.ID)
	maxRetries := 3
	for attempt := 0; attempt <= maxRetries; attempt++ {
		results, err := client.ReadHoldingRegisters(uint16(reg.Address), 1)
		if err != nil {
			waitTime := time.Duration(math.Pow(2, float64(attempt))) * time.Second
			plcLogger.Printf("Error reading holding register %d (attempt %d): %v. Retrying in %v", reg.Address, attempt, err, waitTime)
			select {
			case <-time.After(waitTime):
			case <-ctx.Done():
				return 0, fmt.Errorf("context canceled")
			}
			continue
		}
		if len(results) < 2 {
			err = fmt.Errorf("not enough data returned for register %d", reg.Address)
			waitTime := time.Duration(math.Pow(2, float64(attempt))) * time.Second
			plcLogger.Printf("Error reading holding register %d (attempt %d): %v. Retrying in %v", reg.Address, attempt, err, waitTime)
			select {
			case <-time.After(waitTime):
			case <-ctx.Done():
				return 0, fmt.Errorf("context canceled")
			}
			continue
		}
		value := uint16(results[0])<<8 | uint16(results[1])
		return value, nil
	}
	return 0, fmt.Errorf("failed to read holding register %d after %d retries", reg.Address, maxRetries)
}
func findRegisterByName(registers []Register, name string) *Register {
	for _, reg := range registers {
		if reg.Name == name {
			return &reg // Return a pointer to the found coil
		}
	}
	return nil // Return nil if not found
}

func pollPLC(ctx context.Context, plc PLC) {
	plcLogger := getPLCLogger(plc.ID)
	defer func() {
		if r := recover(); r != nil {
			plcLogger.Printf("Recovered in pollPLC: %v", r)
		}
	}()

	// Initialize states maps.
	stateMutex.Lock()
	plcCoilState[plc.ID] = make(map[int]bool)
	plcInputState[plc.ID] = make(map[int]bool)
	plcRegisterState[plc.ID] = make(map[int]uint16)
	if coilMetrics[plc.ID] == nil {
		coilMetrics[plc.ID] = make(map[int]*CoilMetrics)
	}
	stateMutex.Unlock()

	// Create Modbus TCP handler.
	handler := modbus.NewTCPClientHandler(fmt.Sprintf("%s:%d", plc.IP, plc.Port))
	handler.Timeout = 5 * time.Second
	handler.SlaveId = 1

	// Connect with retry.
	var err error
	maxConnRetries := 3
	for attempt := 0; attempt <= maxConnRetries; attempt++ {
		err = handler.Connect()
		if err == nil {
			break
		}
		waitTime := time.Duration(math.Pow(2, float64(attempt))) * time.Second
		plcLogger.Printf("Error connecting: %v (attempt %d). Retrying in %v", err, attempt, waitTime)
		select {
		case <-time.After(waitTime):
		case <-ctx.Done():
			return
		}
	}
	if err != nil {
		plcLogger.Printf("Failed to connect after %d attempts: %v", maxConnRetries, err)
		return
	}
	defer func() {
		if cerr := handler.Close(); cerr != nil {
			// Handle error if needed.
		}
	}()
	client := modbus.NewClient(handler)

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			plcLogger.Printf("Stopping poller")
			return
		case <-ticker.C:
			// Poll Coils (with metrics)
			for _, coil := range plc.Coils {

				newState, err := readCoilWithRetry(ctx, client, plc, coil)
				if err != nil {
					plcLogger.Printf("Failed to read coil %s: %v", coil.Name, err)
					continue
				}
				stateMutex.RLock()
				oldState, exists := plcCoilState[plc.ID][coil.Address]
				stateMutex.RUnlock()

				stateMutex.Lock()
				if _, ok := coilMetrics[plc.ID][coil.Address]; !ok {
					coilMetrics[plc.ID][coil.Address] = &CoilMetrics{LastChange: time.Now()}
				}
				stateMutex.Unlock()

				if !exists || oldState != newState {
					now := time.Now()
					stateMutex.Lock()
					metrics := coilMetrics[plc.ID][coil.Address]
					delta := now.Sub(metrics.LastChange)
					if exists && oldState {
						metrics.TotalActiveTime += delta
					}
					metrics.LastChange = now
					if newState && (!exists || !oldState) {
						metrics.ActivationCount++
					}
					plcCoilState[plc.ID][coil.Address] = newState
					stateMutex.Unlock()

					for _, tag := range plc.Tower {
						if coil.Name == tag {

							towerLabels := []string{"yellow", "green", "red", "buzzer"}
							towerStates := make(map[string]bool)

							// Find the corresponding coil for this name
							for i, label := range towerLabels {
								register := findRegisterByName(plc.Coils, plc.Tower[i])
								if register != nil {
									towerStates[label] = plcCoilState[plc.ID][register.Address]
								} else {
									towerStates[label] = false // Default to false if not found
								}
							}

							towerEvent := TowerEvent{
								PLCID:  plc.ID,
								Type:   "tower",
								Yellow: towerStates["yellow"],
								Green:  towerStates["green"],
								Red:    towerStates["red"],
								Buzzer: towerStates["buzzer"],
							}

							TowerEventChan <- towerEvent

							log.Printf("Tower States: Yellow: %v, Green: %v, Red: %v, Buzzer: %v",
								towerStates["yellow"], towerStates["green"], towerStates["red"], towerStates["buzzer"])

							log.Printf("%s found in register", coil.Name)
						}
					}

					event := Event{
						PLCID:     plc.ID,
						Type:      "coil",
						Name:      coil.Name,
						Address:   coil.Address,
						OldValue:  oldState,
						NewValue:  newState,
						Timestamp: now,
					}
					EventChan <- event
					plcLogger.Printf("Coil %s changed from %v to %v (%s) - Delta: %v, Activation Count: %d, Total Active Time: %v",
						coil.Name, oldState, newState, coil.Description, delta,
						coilMetrics[plc.ID][coil.Address].ActivationCount,
						coilMetrics[plc.ID][coil.Address].TotalActiveTime)
				}
			}

			// poll Inputs
			for _, reg := range plc.Inputs {
				newValue, err := readInputWithRetry(ctx, client, plc, reg)
				if err != nil {
					plcLogger.Printf("Failed to read input %s: %v", reg.Name, err)
					continue
				}
				stateMutex.RLock()
				oldValue, exists := plcInputState[plc.ID][reg.Address]
				stateMutex.RUnlock()
				if !exists || oldValue != newValue {
					stateMutex.Lock()
					plcInputState[plc.ID][reg.Address] = newValue
					stateMutex.Unlock()

					event := Event{
						PLCID:     plc.ID,
						Type:      "input",
						Name:      reg.Name,
						Address:   reg.Address,
						OldValue:  oldValue,
						NewValue:  newValue,
						Timestamp: time.Now(),
					}

					EventChan <- event
					plcLogger.Printf("Input %s changed from %v to %v", reg.Name, oldValue, newValue)
				}
			}

			// Poll Holding Registers (unchanged)
			for _, reg := range plc.HoldingRegisters {
				newValue, err := readHoldingRegisterWithRetry(ctx, client, plc, reg)
				if err != nil {
					plcLogger.Printf("Failed to read holding register %s: %v", reg.Name, err)
					continue
				}
				stateMutex.RLock()
				oldValue, exists := plcRegisterState[plc.ID][reg.Address]
				stateMutex.RUnlock()
				if !exists || oldValue != newValue {
					stateMutex.Lock()
					plcRegisterState[plc.ID][reg.Address] = newValue
					stateMutex.Unlock()
					event := Event{
						PLCID:     plc.ID,
						Type:      "holding",
						Name:      reg.Name,
						Address:   reg.Address,
						OldValue:  oldValue,
						NewValue:  newValue,
						Timestamp: time.Now(),
					}
					EventChan <- event
					plcLogger.Printf("Holding register %s changed from %v to %v (%s)", reg.Name, oldValue, newValue, reg.Description)
				}
			}
		}
	}
}

func (m *PLCManager) Update(plcs []PLC) {
	m.lock.Lock()
	defer m.lock.Unlock()

	newConfigs := make(map[string]PLC)
	for _, plc := range plcs {
		newConfigs[plc.ID] = plc
	}

	// Stop pollers for removed PLCs.
	for id, cancel := range m.pollers {
		if _, exists := newConfigs[id]; !exists {
			cancel()
			delete(m.pollers, id)
			delete(m.configs, id)
			getPLCLogger(id).Printf("Stopped poller for removed PLC")
		}
	}

	// Start or update pollers.
	for id, newPlc := range newConfigs {
		oldPlc, exists := m.configs[id]
		if !exists || !plcEqual(oldPlc, newPlc) {
			if exists {
				if cancel, ok := m.pollers[id]; ok {
					cancel()
					getPLCLogger(id).Printf("Restarting poller for updated PLC")
				}
			}
		}
		ctx, cancel := context.WithCancel(context.Background())
		m.pollers[id] = cancel
		m.configs[id] = newPlc
		go pollPLC(ctx, newPlc)
		getPLCLogger(id).Printf("Started poller for updated PLC")
	}
}

func LogPLC(plcs []PLC) {
	fmt.Println("PLC Configuration Log:")
	for _, plc := range plcs {
		fmt.Printf("id: \"%s\"\n", plc.ID)
		fmt.Printf("ip: \"%s\"\n", plc.IP)
		fmt.Printf("port: %d\n", plc.Port)
		fmt.Printf("coils: %v\n", plc.Coils)
		fmt.Printf("inputs: %v\n", plc.Inputs)
		fmt.Printf("holdingRegisters: %v\n", plc.HoldingRegisters)
		fmt.Println("----------------------")
	}
}

func GetPLCState(stateType string, plcID string) (string, error) {

	var result any

	switch stateType {

	case "coil":
		if data, ok := plcCoilState[plcID]; ok {
			result = map[string]any{"type": "coil", "plc": plcID, "values": data}
		} else {
			return "", errors.New("coil state not found")
		}
	case "holding":
		if data, ok := plcCoilState[plcID]; ok {
			result = map[string]any{"type": "holding", "plc": plcID, "values": data}
		} else {
			return "", errors.New("coil state not found")
		}
	case "input":
		if data, ok := plcInputState[plcID]; ok {
			result = map[string]any{"type": "input", "plc": plcID, "values": data}
		} else {
			return "", errors.New("input state not found")
		}
	}

	jsonBytes, err := json.Marshal(result)
	if err != nil {
		return "", err
	}

	return string(jsonBytes), nil

}
