package refs

import (
	"io"

	"github.com/go-git/go-git/v5/plumbing/format/pktline"
)

func ReportReceivePackError(w io.Writer, errMessage string) {
	e := pktline.NewEncoder(w)
	e.Encodef("unpack err: %s\n", errMessage)
	e.Flush()
}

func ReportUploadPackError(w io.Writer, errMessage string) {
	e := pktline.NewEncoder(w)
	e.Encodef("NAK\n")
	e.Encodef("%c%s", 3, errMessage)
	e.Flush()
}
