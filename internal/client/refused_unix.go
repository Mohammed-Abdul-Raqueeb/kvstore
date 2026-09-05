//go:build !windows

package client

import (
	"errors"
	"syscall"
)

// isConnRefused reports whether err is a connection-refused dial failure.
//
// On POSIX, a failed connect() surfaces exactly as syscall.ECONNREFUSED, so
// errors.Is against the portable constant works directly.
func isConnRefused(err error) bool {
	return errors.Is(err, syscall.ECONNREFUSED)
}
