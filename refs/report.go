package refs

import (
	"io"

	"github.com/go-git/go-git/v5/plumbing/format/pktline"
)

func ReportUploadPackError(w io.Writer, errMessage string) {
	e := pktline.NewEncoder(w)
	e.Encodef("NAK\n")
	e.Encodef("%c%s", 3, errMessage)
	e.Flush()
}
