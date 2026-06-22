//go:build !windows

package service

import "errors"

func InstallService() error {
	return errors.New("service install is not supported on this platform (Windows only)")
}

func UninstallService() error {
	return errors.New("service uninstall is not supported on this platform (Windows only)")
}

func GetServiceStatus() (string, error) {
	return "", errors.New("service status is not supported on this platform (Windows only)")
}
