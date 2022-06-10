package bdb

import (
	"encoding/binary"
	"fmt"
	"io"
	"strings"
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
	minDelCount   uint64
	maxDelCount   uint64

	firstIndex uint64
	lastIndex  uint64

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

	opts.Logger.Info("leveldb.OpenFile end")

	s := &ldbWALStorage{
		db: db,

		keepLogCount: uint64(opts.KeepLogCount),
		minDelCount:  1000,
		maxDelCount:  10000,

		firstIndex: 0,
		lastIndex:  0,

		logger: opts.Logger,
	}

	if err := s.fillIndex(); err != nil {
		return nil, err
	}

	if err := s.compact(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *ldbWALStorage) fillIndex() error {
	s.logger.Info("fillIndex begin")
	defer s.logger.Info("fillIndex end")

	it := s.db.NewIterator(nil, &opt.ReadOptions{DontFillCache: true})
	defer it.Release()

	if !it.First() {
		if err := it.Error(); err != nil {
			return err
		}
	} else {
		s.firstIndex = binary.BigEndian.Uint64(it.Key())
	}

	if !it.Last() {
		if err := it.Error(); err != nil {
			return err
		}
	} else {
		s.lastIndex = binary.BigEndian.Uint64(it.Key())
	}

	it.Release()
	if err := it.Error(); err != nil {
		return err
	}

	s.logger.Infof("wal: (%d, %d]", s.firstIndex, s.lastIndex)

	return nil
}

func (s *ldbWALStorage) compact() error {
	if s.firstIndex <= 0 {
		return nil
	}

	var limit [8]byte
	binary.BigEndian.PutUint64(limit[:], s.firstIndex)
	compactRange := util.Range{Limit: limit[:]}
	sizeGc, err := s.db.SizeOf([]util.Range{compactRange})
	if err != nil {
		s.logger.Errorf("get size of wal before %d to compact failed, %s", s.firstIndex, err.Error())
		return err
	}
	s.logger.Infof("wal size to gc before compact, %d", sizeGc.Sum())

	if err := s.db.CompactRange(compactRange); err != nil {
		s.logger.Errorf("compact data failed, %s", err.Error())
		return err
	}

	s.logger.Info("compact data success")
	sizeGc, err = s.db.SizeOf([]util.Range{compactRange})
	if err != nil {
		s.logger.Errorf("get size of wal before %d after compact failed, %s", s.firstIndex, err.Error())
		return err
	}
	s.logger.Infof("wal size to gc after compact, %d", sizeGc.Sum())
	return nil
}

func (s *ldbWALStorage) Close() {
	s.db.Close()
	s.db = nil
}

func (s *ldbWALStorage) SetSnapshotIndex(index uint64) {
	atomic.StoreUint64(&s.snapshotIndex, index)
}

func (s *ldbWALStorage) SaveWAL(ents []pb.Entry) error {
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

	del := false
	delReason := ""
	var delBegin, delEnd uint64
	// delete [begin, end)

	first := s.firstIndex
	last := s.lastIndex

	if newFirst, newLast := ents[0].Index, ents[len(ents)-1].Index; newLast < last {
		// delete [ents.back().Index+1, last]
		del = true
		delReason = "trailing"
		delBegin = newLast + 1
		delEnd = last + 1

		if newFirst < first {
			first = newFirst
		}
		last = newLast

	} else if last < newFirst-1 {
		// delete [first-1, last]

		del = true
		delReason = "leading"
		delBegin = first
		delEnd = last + 1

		first = newFirst
		last = newLast

	} else {

		//check if need recycle

		delBegin = first
		delEnd = first

		// increase delEnd
		if delEnd+s.keepLogCount < last {
			delEnd = last - s.keepLogCount
		}

		if delEnd > delBegin+s.maxDelCount {
			delEnd = delBegin + s.maxDelCount
		}

		// keep log before snapshot
		if snapshotIndex := atomic.LoadUint64(&s.snapshotIndex); snapshotIndex > s.keepLogCount &&
			delEnd+s.keepLogCount > snapshotIndex {
			delEnd = snapshotIndex - s.keepLogCount
		}

		if delEnd > delBegin+s.minDelCount {
			del = true
			delReason = "recycling"
			first = delEnd
		} else if first == 0 {
			first = 1
		}

		last = newLast

	}

	var buf strings.Builder
	fmt.Fprintf(&buf, "wal [%d, %d], appending [%d, %d]",
		s.firstIndex, s.lastIndex,
		ents[0].Index, ents[len(ents)-1].Index,
	)

	if del {
		fmt.Fprintf(&buf, ", del_%s [%d, %d)", delReason, delBegin, delEnd)
		for i := delBegin; i < delEnd; i++ {
			var id [8]byte
			binary.BigEndian.PutUint64(id[:], i)
			wb.Delete(id[:])
		}
	}

	fmt.Fprintf(&buf, ", to [%d, %d]", first, last)

	s.logger.Info(buf.String())

	wo := &opt.WriteOptions{
		Sync: true,
	}

	if err := s.db.Write(wb, wo); err != nil {
		return err
	}

	atomic.StoreUint64(&s.lastIndex, last)
	atomic.StoreUint64(&s.firstIndex, first)

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
        return atomic.LoadUint64(&s.firstIndex) + 1, nil
}

func (s *ldbWALStorage) LastIndex() (uint64, error) {
	return atomic.LoadUint64(&s.lastIndex), nil
}

func (s *ldbWALStorage) Describe(w io.Writer) {

	first, last := atomic.LoadUint64(&s.firstIndex), atomic.LoadUint64(&s.lastIndex)

	fmt.Fprintf(w, "wal: (%d, %d]\n", first, last)
}
