package refs

import (
	"errors"
	"os"
	"testing"
)

func TestReportError(t *testing.T) {
	ReportError(os.Stderr, errors.New("my test"))
}
