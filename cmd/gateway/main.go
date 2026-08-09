package main

import (
	"log/slog"
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
		slog.Info("Received termination signal, shutting down...", "component", "main")
		svc.Stop()
	}()

	if err := svc.Run(); err != nil {
		slog.Error("Gateway error", "component", "main", "error", err.Error())
		os.Exit(1)
	}
}
