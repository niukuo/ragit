package refs

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReportReveivePackError(t *testing.T) {
	s := assert.New(t)
	buf := bytes.NewBufferString("")
	ReportReceivePackError(buf, "my test")
	s.EqualValues("0018unpack err: my test\n0000", buf.String())
}

func TestUploadPackError(t *testing.T) {
	s := assert.New(t)
	buf := bytes.NewBufferString("")
	ReportUploadPackError(buf, "my test")
	s.EqualValues("0008NAK\n000c\x03my test0000", buf.String())
}
