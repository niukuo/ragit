// +build go1.12

package gitexec

import (
	"os"
)

func exitCode(p *os.ProcessState) int {
	return p.ExitCode()
}
