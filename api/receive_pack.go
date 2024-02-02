package api

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"time"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/format/packfile"
	"github.com/go-git/go-git/v5/plumbing/format/pktline"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp/capability"
	"github.com/go-git/go-git/v5/plumbing/protocol/packp/sideband"
	"github.com/niukuo/ragit/bdb"
	"github.com/niukuo/ragit/gitexec"
	"github.com/niukuo/ragit/raft"
	"github.com/niukuo/ragit/refs"
)

type sidebandWriter struct {
	channel sideband.Channel
	muxer   *sideband.Muxer
	flusher http.Flusher
}

var _ sideband.Progress = (*sidebandWriter)(nil)

func (w *sidebandWriter) Write(p []byte) (int, error) {
	n, err := w.muxer.WriteChannel(w.channel, p)
	if w.flusher != nil {
		w.flusher.Flush()
	}
	return n, err
}

func (h *httpGitAPI) ReceivePack(hw http.ResponseWriter, r *http.Request) {

	controller := http.NewResponseController(hw)
	// https://github.com/golang/go/issues/15527
	if err := controller.EnableFullDuplex(); err != nil {
		h.logger.Warning("enable full duplex failed, err: ", err)
	}

	var reader io.ReadCloser = r.Body

	if r.ContentLength == 4 {
		c, err := io.ReadAll(reader)
		if err != nil {
			h.logger.Warningf("ioutil.ReadAll failed, err: %v", err)
			hw.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(hw, "read body failed: %s", err.Error())
			return
		}
		if string(c) == "0000" {
			return
		}
		h.logger.Info("content: ", string(c))
		reader = io.NopCloser(bytes.NewReader(c))
	}

	var tee bytes.Buffer
	req := packp.NewReferenceUpdateRequest()
	if err := req.Decode(io.TeeReader(reader, &tee)); err != nil {
		h.logger.Warningf("req.Decode failed, err: %v, data(%d): \n%s", err, tee.Len(), tee.String())
		hw.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(hw, "read request failed: %s", err.Error())
		return
	}

	hw.Header().Set("Content-Type", "application/x-git-receive-pack-result")

	req.Packfile = reader

	var muxer *sideband.Muxer
	if req.Capabilities.Supports(capability.Sideband64k) {
		muxer = sideband.NewMuxer(sideband.Sideband64k, hw)
	} else if req.Capabilities.Supports(capability.Sideband) {
		muxer = sideband.NewMuxer(sideband.Sideband, hw)
	}

	var statusReporter io.Writer = hw
	if muxer != nil {
		sw := &sidebandWriter{
			channel: sideband.ProgressMessage,
			muxer:   muxer,
		}
		sw.flusher, _ = hw.(http.Flusher)
		req.Progress = sw
		statusReporter = muxer
	}

	const ok = "ok"
	reportStatus := packp.NewReportStatus()
	reportStatus.UnpackStatus = ok
	for _, cmd := range req.Commands {
		cmdStatus := &packp.CommandStatus{
			ReferenceName: cmd.Name,
			Status:        ok,
		}
		reportStatus.CommandStatuses = append(reportStatus.CommandStatuses, cmdStatus)
	}

	setErr := func(err error) {
		reportStatus.UnpackStatus = err.Error()
		if reportStatus.UnpackStatus == ok {
			reportStatus.UnpackStatus = "failed"
		}
		for _, cs := range reportStatus.CommandStatuses {
			if cs.Status == ok {
				cs.Status = "canceled"
			}
		}
	}

	if leaderAddrs, err := h.getLeader(r.Context()); err == nil && leaderAddrs != nil {
		hw.Header().Set("Location", fmt.Sprintf("%s/%s/git-receive-pack",
			leaderAddrs[0], path.Base(h.path)))
		hw.WriteHeader(http.StatusTemporaryRedirect)
		return
	} else if err != nil {
		h.logger.Warning("get leader failed, err: ", err)
		setErr(err)
	} else if err := h.receivePack(r.Context(), req, reportStatus); err != nil {
		h.logger.Warning("ReceivePack failed, err: ", err)
		setErr(err)
	}

	if err := reportStatus.Encode(statusReporter); err != nil {
		h.logger.Warning("report status failed: ", err)
	} else if err := pktline.NewEncoder(hw).Flush(); err != nil {
		h.logger.Warning("report status flush failed: ", err)
	}
}

func (h *httpGitAPI) receivePack(ctx context.Context, req *packp.ReferenceUpdateRequest,
	reportStatus *packp.ReportStatus) error {

	var progress sideband.Progress = req.Progress
	if progress == nil {
		progress = io.Discard
	}

	content, err := h.readPack(req.Packfile)
	if err != nil {
		if err == packfile.ErrEmptyPackfile {
		} else {
			fmt.Fprintf(progress, "\x02read pack failed: %s\n", err)
			_, _ = io.Copy(io.Discard, io.LimitReader(req.Packfile, 1*1024*1024))
			return fmt.Errorf("read pack failed: %w", err)
		}
	}

	var refNames []plumbing.ReferenceName
	for _, cmd := range req.Commands {
		refNames = append(refNames, cmd.Name)
	}

	tx, err := h.node.BeginTx(func(txnLocker raft.MapLocker, storage raft.Storage) (
		map[plumbing.ReferenceName]plumbing.Hash, bool, raft.Unlocker, error) {
		return raft.LockRefList(ctx, txnLocker, storage, refNames[0], refNames[1:]...)
	})
	if err != nil {
		return fmt.Errorf("begin tx failed: %w", err)
	}
	defer tx.Close()

	fetch := false
	for i, cmd := range req.Commands {
		if hash := tx.Get(cmd.Name); *hash != cmd.Old {
			fetch = true
			reportStatus.CommandStatuses[i].Status = "fetch first"
		} else {
			*hash = cmd.New
		}
	}
	if fetch {
		return errors.New("fetch first")
	}

	fmt.Fprintf(progress, "\x02packsize: %d, proposing\r", len(content))

	handleCh := make(chan *packp.ReferenceUpdateRequest)
	handleChClosed := false
	defer func() {
		if !handleChClosed {
			close(handleCh)
		}
	}()
	rreq, err := tx.Commit(ctx, content, gitexec.ReqCh(handleCh))
	if err != nil {
		fmt.Fprintf(progress, "\x02packsize: %d, propose failed: %s\n", len(content), err)
		return fmt.Errorf("propose failed: %w", err)
	}

	fmt.Fprintf(progress, "\x02packsize: %d, proposed\n", len(content))

	_, index := rreq.TermIndex()
	chr := []rune{
		'-',
		'\\',
		'|',
		'/',
	}

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for i := 0; ; i++ {
		select {
		case <-ctx.Done():
			fmt.Fprintf(progress, "\x02index: %d, failed on replicating: %s\n", index, err)
			return ctx.Err()

		case <-rreq.Done():
			if err := rreq.Err(); err != nil {
				fmt.Fprintf(progress, "\x02index: %d, failed on replicating: %s\n", index, err)
				return fmt.Errorf("failed on propose: %w", err)
			}
			fmt.Fprintf(progress, "\x02index: %d, no change\n", index)
			return nil

		case <-ticker.C:
			fmt.Fprintf(progress, "\x02index: %d, replicating (%c)\r", index, chr[i%len(chr)])

		case handleCh <- req:
			fmt.Fprintf(progress, "\x02index: %d, replicated\n", index)
			handleChClosed = true
			close(handleCh)
			<-rreq.Done()
			if err := rreq.Err(); err != nil {
				fmt.Fprintf(progress, "\x02apply failed: %s\n", err)
				return fmt.Errorf("apply failed: %w", err)
			} else {
				fmt.Fprint(progress, "\x02applied\n")
			}
			return nil

		}
	}

}

// return nil, nil if I'm leader
func (h *httpGitAPI) getLeader(ctx context.Context) ([]string, error) {

	sm := h.rdb.(bdb.Storage)

	if sm.GetLeaderTerm() != 0 {
		return nil, nil
	}

	status, err := h.node.GetStatus(ctx)
	if err != nil {
		return nil, fmt.Errorf("get status failed: %w", err)
	}

	leaderID := refs.PeerID(status.Lead)

	leaderAddrs, err := sm.GetURLsByMemberID(leaderID)
	if err != nil {
		return nil, fmt.Errorf("get addr for leader %s failed: %w", leaderID, err)
	}

	if len(leaderAddrs) == 0 {
		return nil, fmt.Errorf("leader %s has no addr(s)", leaderID)
	}

	return leaderAddrs, nil
}

var (
	errPackTooLarge = errors.New("object pack too large")
)

func (h *httpGitAPI) readPack(r io.Reader) ([]byte, error) {

	dataBuf := &bytes.Buffer{}

	scanner := packfile.NewScanner(io.TeeReader(r, dataBuf))
	_, objectNum, err := scanner.Header()
	if err != nil {
		return nil, err
	}

	for i := uint32(0); i < objectNum; i++ {
		objectHeader, err := scanner.NextObjectHeader()
		if err != nil {
			return nil, err
		}

		if objectHeader.Length > h.maxFileSize {
			return nil, fmt.Errorf("object too large: %d", objectHeader.Length)
		}

		if objectHeader.Offset > h.maxPackSize {
			return nil, errPackTooLarge
		}
	}

	if _, err := io.Copy(dataBuf, r); err != nil {
		return nil, err
	}

	return dataBuf.Bytes(), nil
}
