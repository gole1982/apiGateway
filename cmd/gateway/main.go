package main

import (
	"log"
	"os"
	"os/signal"
	"syscall"

	"gateway/internal/service"
)

func main() {
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
