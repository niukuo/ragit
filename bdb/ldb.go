package bdb

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync/atomic"

	"github.com/niukuo/ragit/logging"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/util"
	"go.etcd.io/etcd/raft"
	pb "go.etcd.io/etcd/raft/raftpb"
)

type LdbWALStorage = *ldbWALStorage
type ldbWALStorage struct {
	db *leveldb.DB

	snapshotIndex uint64
	keepLogCount  uint64

	logger logging.Logger
}

type WALOptions struct {
	KeepLogCount int
	DBOptions    *opt.Options
	Logger       logging.Logger
}

func NewWALOptions() *WALOptions {
	o := &WALOptions{
		KeepLogCount: 10000,
	}
	return o
}

func OpenWAL(path string, opts *WALOptions) (LdbWALStorage, error) {
	db, err := leveldb.OpenFile(path, opts.DBOptions)
	if err != nil {
		return nil, err
	}
	s := &ldbWALStorage{
		db: db,

		keepLogCount: uint64(opts.KeepLogCount),

		logger: opts.Logger,
	}
	return s, nil
}

func (s *ldbWALStorage) Close() {
	s.db.Close()
	s.db = nil
}

func (s *ldbWALStorage) SetSnapshotIndex(index uint64) {
	atomic.StoreUint64(&s.snapshotIndex, index)
}

func (s *ldbWALStorage) SaveWAL(ents []pb.Entry, sync bool) error {
	if len(ents) == 0 {
		return nil
	}

	wb := new(leveldb.Batch)

	for i := range ents {
		entry := &ents[i]
		expectedIndex := ents[0].Index + uint64(i)
		if entry.Index != expectedIndex {
			return fmt.Errorf("Append entry has gap, expected: %v, actual: %v",
				expectedIndex, entry.Index)
		}
		var id [8]byte
		binary.BigEndian.PutUint64(id[:], entry.Index)
		content, err := entry.Marshal()
		if err != nil {
			return err
		}
		wb.Put(id[:], content)
	}

	it := s.db.NewIterator(nil, nil)
	defer it.Release()

	if !it.First() {
		if err := it.Error(); err != nil {
			return err
		}
	} else {
		del := false
		var delBegin, delEnd uint64
		// delete [begin, end)

		first := binary.BigEndian.Uint64(it.Key())

		if !it.Last() {
			if err := it.Error(); err != nil {
				return err
			}
			return errors.New("get first log failed?")
		}
		last := binary.BigEndian.Uint64(it.Key())

		if last > ents[len(ents)-1].Index {
			// delete [ents.back().Index+1, last]
			del = true
			delBegin = ents[len(ents)-1].Index + 1
			delEnd = last + 1
		} else if last < ents[0].Index-1 {
			// delete [first-1, last]

			del = true
			delBegin = first
			delEnd = last + 1
		} else {
			delBegin = first
			delEnd = first

			// increase delEnd
			if delEnd+s.keepLogCount < last {
				delEnd = last - s.keepLogCount
			}

			// limit max delete to 100
			if delEnd > first+100 {
				delEnd = first + 100
			}

			// keep log before snapshot
			if snapshotIndex := atomic.LoadUint64(&s.snapshotIndex); snapshotIndex > s.keepLogCount &&
				delEnd+s.keepLogCount > snapshotIndex {
				delEnd = snapshotIndex - s.keepLogCount
			}

			if delEnd > delBegin {
				del = true
			}

		}

		if del {
			s.logger.Infof("wal [%d, %d] appending [%d, %d], deleting [%d, %d)",
				first, last,
				ents[0].Index, ents[len(ents)-1].Index,
				delBegin, delEnd,
			)
			for i := delBegin; i < delEnd; i++ {
				var id [8]byte
				binary.BigEndian.PutUint64(id[:], i)
				wb.Delete(id[:])
			}
		}
	}

	wo := &opt.WriteOptions{
		Sync: sync,
	}

	if err := s.db.Write(wb, wo); err != nil {
		return err
	}

	return nil
}

func (s *ldbWALStorage) Entries(lo, hi, maxSize uint64) ([]pb.Entry, error) {
	var begin, end [8]byte
	binary.BigEndian.PutUint64(begin[:], lo)
	binary.BigEndian.PutUint64(end[:], hi)
	slice := &util.Range{
		Start: begin[:],
		Limit: end[:],
	}

	it := s.db.NewIterator(slice, nil)
	defer it.Release()

	size := int(hi - lo)
	entries := make([]pb.Entry, 0, size)
	var total uint64 = 0

	for it.Next() {
		id := binary.BigEndian.Uint64(it.Key())
		if id != lo+uint64(len(entries)) {
			return nil, fmt.Errorf("index gap during get, expected: %v, actual: %v",
				lo+uint64(len(entries)), id)
		}

		var entry pb.Entry
		b := it.Value()
		if err := entry.Unmarshal(b); err != nil {
			return nil, err
		}

		entries = append(entries, entry)

		total += uint64(len(b))
		if total >= maxSize {
			break
		}
	}

	if err := it.Error(); err != nil {
		return nil, err
	}

	return entries, nil
}

func (s *ldbWALStorage) Term(i uint64) (uint64, error) {

	if i == 0 {
		return 0, nil
	}

	var id [8]byte
	binary.BigEndian.PutUint64(id[:], i)

	value, err := s.db.Get(id[:], nil)
	switch err {
	case nil:
		break
	case leveldb.ErrNotFound:
		return 0, raft.ErrCompacted
	default:
		return 0, err
	}

	var entry pb.Entry
	if err := entry.Unmarshal(value); err != nil {
		return 0, err
	}

	return entry.Term, nil
}

func (s *ldbWALStorage) FirstIndex() (uint64, error) {
	it := s.db.NewIterator(nil, nil)
	defer it.Release()

	if it.First() {
		return binary.BigEndian.Uint64(it.Key()) + 1, nil
	}

	if err := it.Error(); err != nil {
		return 0, err
	}

	return 1, nil

}

func (s *ldbWALStorage) LastIndex() (uint64, error) {
	it := s.db.NewIterator(nil, nil)
	defer it.Release()

	if it.Last() {
		return binary.BigEndian.Uint64(it.Key()), nil
	}

	if err := it.Error(); err != nil {
		return 0, err
	}

	return 0, nil
}

func (s *ldbWALStorage) Describe(w io.Writer) {
	it := s.db.NewIterator(nil, nil)
	defer it.Release()

	var first, last uint64

	if it.First() {
		first = binary.BigEndian.Uint64(it.Key())
	}

	if it.Last() {
		last = binary.BigEndian.Uint64(it.Key())
	}

	if err := it.Error(); err != nil {
		fmt.Fprintln(w, "wal: err,", err)
		return
	}

	fmt.Fprintf(w, "wal: (%d, %d]\n", first, last)
}
