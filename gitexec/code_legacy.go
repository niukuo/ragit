// +build !go1.12

package gitexec

import (
	"os"
	"syscall"
)

func exitCode(p *os.ProcessState) int {
	if p == nil {
		return -1
	}
	return p.Sys().(syscall.WaitStatus).ExitStatus()
}
