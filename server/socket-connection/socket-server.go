package socket_connection

import (
	"encoding/json"
	"go-zero/handlers"
	"log"
	"net"
	"sync"
)

var (
	// clients holds active client connections.
	clients      = make(map[net.Conn]struct{})
	clientsMutex sync.Mutex
)

// broadcastTowerEvents listens on the global towerEventChan and sends every event to all connected clients.
func broadcastTowerEvents() {
	for event := range handlers.TowerEventChan {
		data, err := json.Marshal(event)
		if err != nil {
			log.Printf("Error marshaling tower event: %v", err)
			continue
		}
		// Append a newline so clients can separate events.
		data = append(data, '\n')

		clientsMutex.Lock()
		for conn := range clients {
			_, err := conn.Write(data)
			if err != nil {
				log.Printf("Error writing tower event to client %v: %v", conn.RemoteAddr(), err)
				err := conn.Close()
				if err != nil {
					log.Printf("Error closing client connection: %v", err)
				}
				delete(clients, conn)
			}
		}
		clientsMutex.Unlock()
	}
}

// broadcastEvents listens on the global eventChan and sends every event to all connected clients.
func broadcastEvents() {
	for event := range handlers.EventChan {
		data, err := json.Marshal(event)
		if err != nil {
			log.Printf("Error marshaling event: %v", err)
			continue
		}
		// Append a newline so clients can separate events.
		data = append(data, '\n')

		clientsMutex.Lock()
		for conn := range clients {
			_, err := conn.Write(data)
			if err != nil {
				log.Printf("Error writing to client %v: %v", conn.RemoteAddr(), err)
				err := conn.Close()
				if err != nil {
					return
				}
				delete(clients, conn)
			}
		}
		clientsMutex.Unlock()
	}
}

// StartSocketServer listens on TCP port 9090 and accepts incoming client connections.
func StartSocketServer() {
	ln, err := net.Listen("tcp", ":9090")
	if err != nil {
		log.Printf("Error starting socket server: %v", err)
		return
	}
	log.Printf("Socket server started on :9090")

	// Start goroutines to broadcast both types of events.
	//go broadcastEvents()      // Handles general events from eventChan
	go broadcastTowerEvents() // Handles tower events from towerEventChan

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("Error accepting connection: %v", err)
			continue
		}

		// Add the new client connection to the map
		clientsMutex.Lock()
		clients[conn] = struct{}{}
		clientsMutex.Unlock()
		log.Printf("New client connected: %v", conn.RemoteAddr())

		// Start a goroutine to handle client connection (e.g., disconnection handling)
		go handleConnection(conn)
	}
}

// handleConnection monitors a client connection for disconnection by reading from it.
func handleConnection(conn net.Conn) {
	buf := make([]byte, 1)
	for {
		_, err := conn.Read(buf)
		if err != nil {
			break
		}
	}
	// Remove the client when the connection is closed.
	clientsMutex.Lock()
	delete(clients, conn)
	clientsMutex.Unlock()
	log.Printf("Client disconnected: %v", conn.RemoteAddr())
	err := conn.Close()
	if err != nil {
		return
	}
}
