package logging

import (
	"testing"
)

func TestLogging(t *testing.T) {
	logger := GetLogger("myprefix")
	logger.Debug("hahatest", "hehetest")
}
