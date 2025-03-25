package main

import "sync"

// Global state maps.
var (
	plcCoilState     = make(map[string]map[int]bool)
	plcInputState    = make(map[string]map[int]bool)
	plcRegisterState = make(map[string]map[int]uint16)
	coilMetrics      = make(map[string]map[int]*CoilMetrics)
	stateMutex       sync.RWMutex
	eventChan        = make(chan Event, 1000)
	towerEventChan   = make(chan TowerEvent, 200)
)
