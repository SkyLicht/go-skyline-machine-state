package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/goburrow/modbus"
	"strings"
	"sync"
	"time"
)

// -----------------------------
// Global Instances & Initialization
// -----------------------------

// GlobalPLCManager is the single shared instance of PLCManager.
var GlobalPLCManager = NewPLCManager()

// -----------------------------
// Data Types and Global State
// -----------------------------

// Event represents a change in a coil, input, or holding register value.
type Event struct {
	PLCID     string      `json:"plc_id"`
	Type      string      `json:"type"` // "coil", "input", or "holding"
	Name      string      `json:"name"`
	Address   int         `json:"address"`
	OldValue  interface{} `json:"old_value"`
	NewValue  interface{} `json:"new_value"`
	Timestamp time.Time   `json:"timestamp"`
}

// TowerEvent represents a tower state event.
type TowerEvent struct {
	PLCID     string    `json:"plc_id"`
	Type      string    `json:"type"`
	Yellow    bool      `json:"yellow"`
	Green     bool      `json:"green"`
	Red       bool      `json:"red"`
	Buzzer    bool      `json:"buzzer"`
	Timestamp time.Time `json:"timestamp"`
}

// PLCDisconnectEvent is emitted when a PLC disconnects.
type PLCDisconnectEvent struct {
	PLCID     string    `json:"plc_id"`
	Timestamp time.Time `json:"timestamp"`
	Reason    string    `json:"reason"`
}

// Global channel for PLC disconnect events.
var PLCDisconnectEventChan = make(chan PLCDisconnectEvent, 100)

// Register represents a PLC register (coil, input, or holding register).
type Register struct {
	Name        string `json:"name"`
	Address     int    `json:"address"`
	Description string `json:"desc"`
}

// PLC represents the configuration for a PLC.
type PLC struct {
	ID               string     `json:"id"`
	Owner            string     `json:"owner"`
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

// Global state maps for PLC data.
var (
	plcCoilState     = make(map[string]map[int]bool)
	plcInputState    = make(map[string]map[int]bool)
	plcRegisterState = make(map[string]map[int]uint16)
	coilMetrics      = make(map[string]map[int]*CoilMetrics)
	stateMutex       sync.RWMutex
	EventChan        = make(chan Event, 1000)
	TowerEventChan   = make(chan TowerEvent, 200)
)

// -----------------------------
// PLCManager: Manages Pollers & Configurations
// -----------------------------

// PLCManager manages active pollers and PLC configurations.
type PLCManager struct {
	mu      sync.RWMutex
	pollers map[string]context.CancelFunc
	configs map[string]PLC
}

// NewPLCManager creates and returns a new PLCManager instance.
func NewPLCManager() *PLCManager {
	return &PLCManager{
		pollers: make(map[string]context.CancelFunc),
		configs: make(map[string]PLC),
	}
}

// Update replaces the current PLC configurations with the provided list.
// It stops pollers for removed PLCs and starts or restarts pollers for new or updated PLCs.
func (m *PLCManager) Update(plcs []PLC) {
	newConfigs := make(map[string]PLC)
	for _, plc := range plcs {
		newConfigs[plc.ID] = plc
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	// Stop pollers for PLCs that no longer exist.
	for id, cancel := range m.pollers {
		if _, exists := newConfigs[id]; !exists {
			cancel()
			delete(m.pollers, id)
			delete(m.configs, id)
		}
	}

	// Start or update pollers for new or modified PLCs.
	for id, newPlc := range newConfigs {
		oldPlc, exists := m.configs[id]
		if !exists || !plcEqual(oldPlc, newPlc) {
			// If a poller already exists, cancel it.
			if exists {
				if cancel, ok := m.pollers[id]; ok {
					cancel()
				}
			}
			// Store new configuration.
			m.configs[id] = newPlc
			// Start a new poller.
			ctx, cancel := context.WithCancel(context.Background())
			m.pollers[id] = cancel
			go pollPLC(ctx, newPlc)
		}
	}
}

// GetConfig returns the PLC configuration for the given plcID.
func (m *PLCManager) GetConfig(plcID string) (PLC, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	plc, exists := m.configs[plcID]
	return plc, exists
}

// DisconnectPLC manually disconnects the PLC by canceling its poller.
func (m *PLCManager) DisconnectPLC(plcID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	cancel, exists := m.pollers[plcID]
	if !exists {
		return fmt.Errorf("PLC %s is not currently connected", plcID)
	}
	cancel()
	delete(m.pollers, plcID)
	getPLCLogger(plcID).Printf("PLC %s manually disconnected", plcID)
	// Emit disconnect event.
	PLCDisconnectEventChan <- PLCDisconnectEvent{
		PLCID:     plcID,
		Timestamp: time.Now(),
		Reason:    "manual disconnect",
	}
	return nil
}

// ReconnectPLC manually attempts to reconnect a PLC.
func (m *PLCManager) ReconnectPLC(plcID string) error {
	m.mu.Lock()
	_, connected := m.pollers[plcID]
	config, exists := m.configs[plcID]
	m.mu.Unlock()
	if connected {
		return fmt.Errorf("PLC %s is already connected", plcID)
	}
	if !exists {
		// Optionally, check a failed connection store if needed.
		return fmt.Errorf("PLC %s configuration not found", plcID)
	}
	ctx, cancel := context.WithCancel(context.Background())
	m.mu.Lock()
	m.pollers[plcID] = cancel
	m.configs[plcID] = config
	m.mu.Unlock()
	go pollPLC(ctx, config)
	getPLCLogger(plcID).Printf("PLC %s manually reconnected", plcID)
	return nil
}

// plcEqual checks if two PLC configurations are equal.
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

// -----------------------------
// Helper Functions
// -----------------------------

// exponentialBackoff returns a time.Duration based on the attempt number.
func exponentialBackoff(attempt int) time.Duration {
	return time.Duration(1<<attempt) * time.Second
}

// isFatalConnectionError checks if an error indicates a fatal connection error.
func isFatalConnectionError(err error) bool {
	return err != nil && strings.Contains(err.Error(), "forcibly closed")
}

// -----------------------------
// Global Failed PLC Tracking & Reconnection
// -----------------------------

var (
	failedPLCConnections = make(map[string]PLC)
	failedPLCMutex       sync.Mutex
)

// storeFailedPLC saves the PLC configuration in the global failed connection map.
func storeFailedPLC(plc PLC) {
	failedPLCMutex.Lock()
	defer failedPLCMutex.Unlock()
	failedPLCConnections[plc.ID] = plc
	getPLCLogger(plc.ID).Printf("Stored PLC %s for reconnection attempts", plc.ID)
}

// attemptReconnect tries to connect to the PLC; returns true if successful.
func attemptReconnect(plc PLC) bool {
	plcLogger := getPLCLogger(plc.ID)
	handler := modbus.NewTCPClientHandler(fmt.Sprintf("%s:%d", plc.IP, plc.Port))
	handler.Timeout = 5 * time.Second
	handler.SlaveId = 1
	err := handler.Connect()
	if err != nil {
		plcLogger.Printf("Reconnect attempt failed for PLC %s: %v", plc.ID, err)
		return false
	}
	handler.Close()
	plcLogger.Printf("Reconnect attempt succeeded for PLC %s", plc.ID)
	return true
}

// reconnectFailedPLCs periodically attempts to reconnect failed PLCs.
func reconnectFailedPLCs() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	for {
		<-ticker.C
		failedPLCMutex.Lock()
		for id, plc := range failedPLCConnections {
			if attemptReconnect(plc) {
				getPLCLogger(plc.ID).Printf("Reconnected successfully, starting poller.")
				delete(failedPLCConnections, id)
				ctx, cancel := context.WithCancel(context.Background())
				GlobalPLCManager.mu.Lock()
				GlobalPLCManager.pollers[plc.ID] = cancel
				GlobalPLCManager.configs[plc.ID] = plc
				GlobalPLCManager.mu.Unlock()
				go pollPLC(ctx, plc)
			}
		}
		failedPLCMutex.Unlock()
	}
}

func init() {
	go reconnectFailedPLCs()
}

// -----------------------------
// Polling & Read Functions
// -----------------------------

// findRegisterByName returns a pointer to the first register in the slice that matches the provided name.
func findRegisterByName(registers []Register, name string) *Register {
	for _, reg := range registers {
		if reg.Name == name {
			return &reg
		}
	}
	return nil
}

// pollPLC continuously polls a PLC for its state.
func pollPLC(ctx context.Context, plc PLC) {
	plcLogger := getPLCLogger(plc.ID)
	defer func() {
		if r := recover(); r != nil {
			plcLogger.Printf("Recovered in pollPLC: %v", r)
		}
	}()

	// Initialize state maps.
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

	// Connect with retry logic.
	maxConnRetries := 3
	var err error
	for attempt := 0; attempt <= maxConnRetries; attempt++ {
		err = handler.Connect()
		if err == nil {
			break
		}
		waitTime := exponentialBackoff(attempt)
		plcLogger.Printf("Error connecting: %v (attempt %d). Retrying in %v", err, attempt, waitTime)
		select {
		case <-time.After(waitTime):
		case <-ctx.Done():
			return
		}
	}
	if err != nil {
		plcLogger.Printf("Failed to connect after %d attempts: %v", maxConnRetries, err)
		storeFailedPLC(plc)
		PLCDisconnectEventChan <- PLCDisconnectEvent{
			PLCID:     plc.ID,
			Timestamp: time.Now(),
			Reason:    fmt.Sprintf("Failed to connect: %v", err),
		}
		return
	}
	defer func() {
		if cerr := handler.Close(); cerr != nil {
			plcLogger.Printf("Error closing handler: %v", cerr)
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
			// Poll Coils
			for _, coil := range plc.Coils {
				newState, err := readCoilWithRetry(ctx, client, plc, coil)
				if err != nil {
					if isFatalConnectionError(err) {
						plcLogger.Printf("Fatal connection error for coil %s: %v. Stopping poller.", coil.Name, err)
						PLCDisconnectEventChan <- PLCDisconnectEvent{
							PLCID:     plc.ID,
							Timestamp: time.Now(),
							Reason:    fmt.Sprintf("Fatal error reading coil %s: %v", coil.Name, err),
						}
						storeFailedPLC(plc)
						return
					}
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
					// Emit event (tower events omitted for brevity).
					EventChan <- Event{
						PLCID:     plc.ID,
						Type:      "coil",
						Name:      coil.Name,
						Address:   coil.Address,
						OldValue:  oldState,
						NewValue:  newState,
						Timestamp: now,
					}

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
							//
							//log.Printf("Tower States: Yellow: %v, Green: %v, Red: %v, Buzzer: %v",
							//	towerStates["yellow"], towerStates["green"], towerStates["red"], towerStates["buzzer"])
							//
							//log.Printf("%s found in register", coil.Name)
						}
					}

					plcLogger.Printf("Coil %s changed from %v to %v", coil.Name, oldState, newState)
				}
			}
			// Poll Inputs
			for _, reg := range plc.Inputs {
				newValue, err := readInputWithRetry(ctx, client, plc, reg)
				if err != nil {
					if isFatalConnectionError(err) {
						plcLogger.Printf("Fatal connection error for input %s: %v. Stopping poller.", reg.Name, err)
						PLCDisconnectEventChan <- PLCDisconnectEvent{
							PLCID:     plc.ID,
							Timestamp: time.Now(),
							Reason:    fmt.Sprintf("Fatal error reading input %s: %v", reg.Name, err),
						}
						storeFailedPLC(plc)
						return
					}
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
					EventChan <- Event{
						PLCID:     plc.ID,
						Type:      "input",
						Name:      reg.Name,
						Address:   reg.Address,
						OldValue:  oldValue,
						NewValue:  newValue,
						Timestamp: time.Now(),
					}
					plcLogger.Printf("Input %s changed from %v to %v", reg.Name, oldValue, newValue)
				}
			}
			// Poll Holding Registers
			for _, reg := range plc.HoldingRegisters {
				newValue, err := readHoldingRegisterWithRetry(ctx, client, plc, reg)
				if err != nil {
					if isFatalConnectionError(err) {
						plcLogger.Printf("Fatal connection error for holding register %s: %v. Stopping poller.", reg.Name, err)
						PLCDisconnectEventChan <- PLCDisconnectEvent{
							PLCID:     plc.ID,
							Timestamp: time.Now(),
							Reason:    fmt.Sprintf("Fatal error reading holding register %s: %v", reg.Name, err),
						}
						storeFailedPLC(plc)
						return
					}
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
					EventChan <- Event{
						PLCID:     plc.ID,
						Type:      "holding",
						Name:      reg.Name,
						Address:   reg.Address,
						OldValue:  oldValue,
						NewValue:  newValue,
						Timestamp: time.Now(),
					}
					plcLogger.Printf("Holding register %s changed from %v to %v", reg.Name, oldValue, newValue)
				}
			}
		}
	}
}

// readInputWithRetry reads a discrete input with retry logic.
func readInputWithRetry(ctx context.Context, client modbus.Client, plc PLC, input Register) (bool, error) {
	plcLogger := getPLCLogger(plc.ID)
	maxRetries := 3
	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		results, err := client.ReadDiscreteInputs(uint16(input.Address), 1)
		if err != nil {
			if isFatalConnectionError(err) {
				return false, err
			}
			lastErr = err
			waitTime := exponentialBackoff(attempt)
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
	return false, fmt.Errorf("failed to read input %d after %d retries: last error: %v", input.Address, maxRetries, lastErr)
}

// readCoilWithRetry reads a coil with retry logic.
func readCoilWithRetry(ctx context.Context, client modbus.Client, plc PLC, coil Register) (bool, error) {
	plcLogger := getPLCLogger(plc.ID)
	maxRetries := 3
	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		results, err := client.ReadCoils(uint16(coil.Address), 1)
		if err != nil {
			if isFatalConnectionError(err) {
				return false, err
			}
			lastErr = err
			waitTime := exponentialBackoff(attempt)
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
	return false, fmt.Errorf("failed to read coil %d after %d retries: last error: %v", coil.Address, maxRetries, lastErr)
}

// readHoldingRegisterWithRetry reads a holding register with retry logic.
func readHoldingRegisterWithRetry(ctx context.Context, client modbus.Client, plc PLC, reg Register) (uint16, error) {
	plcLogger := getPLCLogger(plc.ID)
	maxRetries := 3
	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		results, err := client.ReadHoldingRegisters(uint16(reg.Address), 1)
		if err != nil {
			if isFatalConnectionError(err) {
				return 0, err
			}
			lastErr = err
			waitTime := exponentialBackoff(attempt)
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
			if isFatalConnectionError(err) {
				return 0, err
			}
			lastErr = err
			waitTime := exponentialBackoff(attempt)
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
	return 0, fmt.Errorf("failed to read holding register %d after %d retries: last error: %v", reg.Address, maxRetries, lastErr)
}

// -----------------------------
// Detailed State Retrieval
// -----------------------------

// GetDetailedPLCState returns a detailed JSON state including register metadata for the specified PLC.
func GetDetailedPLCState(plcID string) (string, error) {
	stateMutex.RLock()
	defer stateMutex.RUnlock()

	plc, exists := GlobalPLCManager.GetConfig(plcID)
	if !exists {
		return "", fmt.Errorf("PLC %s not found in config", plcID)
	}

	var coils []map[string]interface{}
	for _, coil := range plc.Coils {
		coils = append(coils, map[string]interface{}{
			"name":        coil.Name,
			"address":     coil.Address,
			"description": coil.Description,
			"state":       plcCoilState[plcID][coil.Address],
		})
	}

	var inputs []map[string]interface{}
	for _, input := range plc.Inputs {
		inputs = append(inputs, map[string]interface{}{
			"name":        input.Name,
			"address":     input.Address,
			"description": input.Description,
			"state":       plcInputState[plcID][input.Address],
		})
	}

	var registers []map[string]interface{}
	for _, reg := range plc.HoldingRegisters {
		registers = append(registers, map[string]interface{}{
			"name":        reg.Name,
			"address":     reg.Address,
			"description": reg.Description,
			"value":       plcRegisterState[plcID][reg.Address],
		})
	}

	result := map[string]interface{}{
		"plc":       plcID,
		"coils":     coils,
		"inputs":    inputs,
		"registers": registers,
	}

	jsonBytes, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return "", err
	}
	return string(jsonBytes), nil
}

// GetTowerStateGroupedByOwner returns the tower state for each PLC grouped by Owner.
// It creates a JSON string where each owner maps to a list of PLC tower states.
func GetTowerStateGroupedByOwner() (string, error) {
	// The expected tower labels in order.
	towerLabels := []string{"yellow", "green", "red", "buzzer"}

	// This will group the tower state info by Owner.
	result := make(map[string][]map[string]interface{})

	// Lock the GlobalPLCManager for reading the PLC configurations.
	GlobalPLCManager.mu.RLock()
	defer GlobalPLCManager.mu.RUnlock()

	// Iterate over each configured PLC.
	for _, plc := range GlobalPLCManager.configs {
		// Build the tower state for this PLC.
		towerState := make(map[string]bool)
		for i, label := range towerLabels {
			if i < len(plc.Tower) {
				// Get the register name for this tower state.
				regName := plc.Tower[i]
				// Find the corresponding register in the PLC's Coils.
				reg := findRegisterByName(plc.Coils, regName)
				if reg != nil {
					// Retrieve the state from the global state map.
					stateMutex.RLock()
					state := plcCoilState[plc.ID][reg.Address]
					stateMutex.RUnlock()
					towerState[label] = state
				} else {
					towerState[label] = false
				}
			} else {
				towerState[label] = false
			}
		}

		// Create an object with the PLC id and its tower state.
		plcTowerInfo := map[string]interface{}{
			"plc_id": plc.ID,
			"tower":  towerState,
		}

		// Group the result by Owner.
		owner := plc.Owner
		result[owner] = append(result[owner], plcTowerInfo)
	}

	// Marshal the result to indented JSON.
	jsonBytes, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		return "", err
	}
	return string(jsonBytes), nil
}
