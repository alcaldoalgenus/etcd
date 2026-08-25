package mvcc

import (
	"bytes"
	"context"
	"errors"
	"math"
)

var (
	// ErrCompacted is returned when requesting a revision that has been compacted.
	ErrCompacted = errors.New("mvcc: required revision has been compacted")
	// ErrKeyNotFound is returned when a key does not exist.
	ErrKeyNotFound = errors.New("mvcc: key not found")
)

type storeTxnRead struct {
	s       *store
	readRev int64
}

func (tr *storeTxnRead) Rev() int64 {
	return tr.readRev
}

func (tr *storeTxnRead) Range(ctx context.Context, key, end []byte, ro RangeOptions) (*RangeResult, error) {
	return tr.rangeKeys(ctx, key, end, tr.readRev, ro)
}

func (tr *storeTxnRead) rangeKeys(ctx context.Context, key, end []byte, curRev int64, ro RangeOptions) (*RangeResult, error) {
	limit := int(ro.Limit)
	if limit <= 0 {
		limit = math.MaxInt32
	}

	tr.s.mu.RLock()
	defer tr.s.mu.RUnlock()

	matched := make([]KeyValue, 0)
	for _, item := range tr.s.items {
		if item.ModRevision > curRev {
			continue
		}
		if ro.MinModRev > 0 && item.ModRevision < ro.MinModRev {
			continue
		}
		if ro.MaxModRev > 0 && item.ModRevision > ro.MaxModRev {
			continue
		}
		if ro.MinCreateRev > 0 && item.CreateRevision < ro.MinCreateRev {
			continue
		}
		if ro.MaxCreateRev > 0 && item.CreateRevision > ro.MaxCreateRev {
			continue
		}

		inRange := false
		if len(end) == 0 {
			inRange = bytes.Equal(item.Key, key)
		} else if bytes.Equal(end, []byte{0}) {
			inRange = bytes.Compare(item.Key, key) >= 0
		} else {
			inRange = bytes.Compare(item.Key, key) >= 0 && bytes.Compare(item.Key, end) < 0
		}

		if inRange {
			matched = append(matched, item)
		}
	}

	total := len(matched)
	if ro.CountOnly {
		return &RangeResult{
			KVs:   nil,
			Count: total,
			Rev:   curRev,
		}, nil
	}

	kvs := make([]KeyValue, 0, len(matched))
	more := false
	for _, item := range matched {
		if len(kvs) >= limit {
			more = true
			break
		}
		kv := KeyValue{
			Key:            item.Key,
			CreateRevision: item.CreateRevision,
			ModRevision:    item.ModRevision,
			Version:        item.Version,
			Lease:          item.Lease, // Preserve Lease ID even when KeysOnly is true
		}
		if !ro.KeysOnly {
			kv.Value = item.Value
		}
		kvs = append(kvs, kv)
	}

	return &RangeResult{
		KVs:   kvs,
		Count: total,
		Rev:   curRev,
		More:  more,
	}, nil
}

func (tr *storeTxnRead) End() {
}
