package main

import (
	"go-zero/handlers"
	"go-zero/util"
	"log"
)

func main() {
	handlers.EnsureLogDir() // Ensure the folder exists before writing any log files.
	// Load the initial PLC configuration from file.
	plcs, err := util.LoadJsonFile[handlers.PLC]("config.json")
	if err != nil {
		log.Fatalf("Failed to load PLC configuration: %v", err)
	}
	//
	// Create a PLCManager and start pollers for each PLC.
	plcManager := handlers.NewPLCManager()
	plcManager.Update(plcs)

	handlers.LogPLC(plcs)

	// Start the socket server (defined in socket_server.go).
	//go socket_connection.StartSocketServer()
	//
	//go api.RunServer()

	// Keep the application running.
	select {}

	//// Background goroutine to reload configuration every 30 seconds.
	//go func() {
	//	ticker := time.NewTicker(30 * time.Second)
	//	defer ticker.Stop()
	//	for {
	//		select {
	//		case <-ticker.C:
	//			newPlcs, err := loadJsonFile[PLC]("config.json")
	//			if err != nil {
	//				log.Printf("Error reloading config: %v", err)
	//				continue
	//			}
	//			plcManager.Update(newPlcs)
	//		}
	//	}
	//}()

}
