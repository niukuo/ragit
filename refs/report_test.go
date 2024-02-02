package refs

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUploadPackError(t *testing.T) {
	s := assert.New(t)
	buf := bytes.NewBufferString("")
	ReportUploadPackError(buf, "my test")
	s.EqualValues("0008NAK\n000c\x03my test0000", buf.String())
}
