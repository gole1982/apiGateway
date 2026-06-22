package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"gateway/internal/service"
)

var (
	installFlag    = flag.Bool("install", false, "Install as Windows Service")
	uninstallFlag  = flag.Bool("uninstall", false, "Uninstall Windows Service")
	serviceFlag    = flag.Bool("service", false, "Run as Windows Service")
)

func main() {
	flag.Parse()

	if *installFlag {
		if err := service.InstallService(); err != nil {
			log.Fatalf("Failed to install service: %v", err)
		}
		log.Println("Service installed successfully")
		return
	}

	if *uninstallFlag {
		if err := service.UninstallService(); err != nil {
			log.Fatalf("Failed to uninstall service: %v", err)
		}
		log.Println("Service uninstalled successfully")
		return
	}

	if *serviceFlag || isRunningAsService() {
		svc := service.New()
		if err := svc.Run(); err != nil {
			log.Fatalf("Service error: %v", err)
		}
		return
	}

	// Run as console application
	runConsole()
}

func isRunningAsService() bool {
	// Check if running in Windows Service environment
	session, _ := os.LookupEnv("SESSIONNAME")
	return strings.ToUpper(session) == "SERVICES"
}

func runConsole() {
	log.Println("Starting API Gateway in console mode...")
	svc := service.New()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigCh
		log.Println("Received termination signal, shutting down...")
		svc.Stop()
	}()

	if err := svc.Run(); err != nil {
		log.Fatalf("Gateway error: %v", err)
	}
}
