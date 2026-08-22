package service

// Version is the gateway build version. It follows the Go convention of
// injecting the value at build time from a git tag, e.g.:
//
//	go build -ldflags "-X gateway/internal/service.Version=$(git describe --tags)"
//
// Local development builds without the flag fall back to "dev".
var Version = "dev"
