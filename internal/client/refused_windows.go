//go:build windows

package client

import (
	"errors"
	"syscall"
)

// wsaeConnRefused is WSAECONNREFUSED, the raw Winsock error for a refused
// connection.
//
// It is deliberately not syscall.ECONNREFUSED: on Windows that symbol is
// one of Go's own pseudo-errno values (defined relative to
// APPLICATION_ERROR, for source code that wants a POSIX-style name to
// return from its own functions) and is never what a real connect()
// failure actually produces. A refused TCP connect comes back as this raw
// WSA code untranslated, so errors.Is(err, syscall.ECONNREFUSED) silently
// never matches a genuine Windows dial failure — comparing against the
// real code instead.
const wsaeConnRefused = syscall.Errno(10061)

func isConnRefused(err error) bool {
	return errors.Is(err, wsaeConnRefused)
}
