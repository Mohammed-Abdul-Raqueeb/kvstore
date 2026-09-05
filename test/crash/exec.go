package crash

import "os/exec"

// startCmd builds the server command line used by tryStart. Kept separate so
// the test body stays readable.
func startCmd(bin, addr, dataDir string) *exec.Cmd {
	return exec.Command(bin, "--addr", addr, "--data-dir", dataDir, "--fsync", "always", "--log-level", "warn")
}
