//go:build windows

package service

import (
	"fmt"
	"os"
	"os/exec"
	"strings"
	"syscall"
)

func InstallService() error {
	exePath, err := os.Executable()
	if err != nil {
		return err
	}

	cmd := exec.Command("sc", "create", "GatewayService",
		"binPath=", exePath,
		"DisplayName=", "API Gateway Service",
		"start=", "auto",
		"type=", "own")
	cmd.SysProcAttr = &syscall.SysProcAttr{
		HideWindow: true,
	}
	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("sc create failed: %v, output: %s", err, string(output))
	}
	return nil
}

func UninstallService() error {
	cmd := exec.Command("sc", "delete", "GatewayService")
	cmd.SysProcAttr = &syscall.SysProcAttr{
		HideWindow: true,
	}
	output, err := cmd.CombinedOutput()
	if err != nil && !strings.Contains(string(output), "does not exist") {
		return fmt.Errorf("sc delete failed: %v, output: %s", err, string(output))
	}
	return nil
}

func GetServiceStatus() (string, error) {
	cmd := exec.Command("sc", "query", "GatewayService", "state=", "=")
	cmd.SysProcAttr = &syscall.SysProcAttr{
		HideWindow: true,
	}
	output, err := cmd.CombinedOutput()
	if err != nil {
		return "", err
	}
	return string(output), nil
}