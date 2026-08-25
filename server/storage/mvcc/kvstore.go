package mvcc

import (
	"bytes"
	"context"
	"sync"
)

// Store defines the MVCC key-value storage interface.
type Store interface {
	Read(rev int64) TxnRead
	Write() TxnWrite
	Put(key, value []byte, lease int64) int64
	Range(ctx context.Context, key, end []byte, ro RangeOptions) (*RangeResult, error)
	DeleteRange(key, end []byte) (n int64, rev int64)
	Rev() int64
	Close() error
}

// TxnRead defines read transaction operations.
type TxnRead interface {
	Rev() int64
	Range(ctx context.Context, key, end []byte, ro RangeOptions) (*RangeResult, error)
	End()
}

// TxnWrite defines write transaction operations.
type TxnWrite interface {
	TxnRead
	Put(key, value []byte, lease int64) int64
	DeleteRange(key, end []byte) (n int64, rev int64)
	End()
}

type store struct {
	mu          sync.RWMutex
	currentRev  int64
	items       []KeyValue
	compactMain int64
}

// NewStore initializes a new MVCC Store instance.
func NewStore() Store {
	return &store{
		currentRev: 1,
		items:      make([]KeyValue, 0),
	}
}

func (s *store) Rev() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.currentRev
}

func (s *store) Read(rev int64) TxnRead {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if rev <= 0 || rev > s.currentRev {
		rev = s.currentRev
	}
	return &storeTxnRead{
		s:       s,
		readRev: rev,
	}
}

func (s *store) Write() TxnWrite {
	s.mu.Lock()
	return &storeTxnWrite{
		storeTxnRead: storeTxnRead{
			s:       s,
			readRev: s.currentRev + 1,
		},
	}
}

func (s *store) Put(key, value []byte, lease int64) int64 {
	tw := s.Write()
	rev := tw.Put(key, value, lease)
	tw.End()
	return rev
}

func (s *store) Range(ctx context.Context, key, end []byte, ro RangeOptions) (*RangeResult, error) {
	tr := s.Read(0)
	defer tr.End()
	return tr.Range(ctx, key, end, ro)
}

func (s *store) DeleteRange(key, end []byte) (n int64, rev int64) {
	tw := s.Write()
	n, rev = tw.DeleteRange(key, end)
	tw.End()
	return n, rev
}

func (s *store) Close() error {
	return nil
}

type storeTxnWrite struct {
	storeTxnRead
}

func (tw *storeTxnWrite) Put(key, value []byte, lease int64) int64 {
	s := tw.s
	s.currentRev++
	rev := s.currentRev

	var (
		createRev int64 = rev
		ver       int64 = 1
		foundIdx  int   = -1
	)

	for i, item := range s.items {
		if bytes.Equal(item.Key, key) {
			createRev = item.CreateRevision
			ver = item.Version + 1
			foundIdx = i
			break
		}
	}

	newKV := KeyValue{
		Key:            append([]byte(nil), key...),
		Value:          append([]byte(nil), value...),
		CreateRevision: createRev,
		ModRevision:    rev,
		Version:        ver,
		Lease:          lease,
	}

	if foundIdx >= 0 {
		s.items[foundIdx] = newKV
	} else {
		s.items = append(s.items, newKV)
	}

	return rev
}

func (tw *storeTxnWrite) DeleteRange(key, end []byte) (n int64, rev int64) {
	s := tw.s
	s.currentRev++
	rev := s.currentRev

	var newItems []KeyValue
	for _, item := range s.items {
		inRange := false
		if len(end) == 0 {
			inRange = bytes.Equal(item.Key, key)
		} else if bytes.Equal(end, []byte{0}) {
			inRange = bytes.Compare(item.Key, key) >= 0
		} else {
			inRange = bytes.Compare(item.Key, key) >= 0 && bytes.Compare(item.Key, end) < 0
		}

		if inRange {
			n++
		} else {
			newItems = append(newItems, item)
		}
	}

	s.items = newItems
	return n, rev
}

func (tw *storeTxnWrite) End() {
	tw.s.mu.Unlock()
}
