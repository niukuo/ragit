package refs

import (
	"io"

	"github.com/go-git/go-git/v5/plumbing/format/pktline"
)

func ReportError(w io.Writer, err error) {
	e := pktline.NewEncoder(w)
	e.Encodef("unpack err: %s\n", err.Error())
	e.Flush()
}
