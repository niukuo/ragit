package refs

import (
	"io"

	"gopkg.in/src-d/go-git.v4/plumbing/format/pktline"
)

func ReportError(w io.Writer, err error) {
	e := pktline.NewEncoder(w)
	e.Encodef("unpack err: %s\n", err.Error())
	e.Flush()
}
