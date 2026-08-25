package mvcc

import (
	"context"
	"testing"
)

func TestRangeKeysOnlyPreservesLease(t *testing.T) {
	s := NewStore()
	defer s.Close()

	ctx := context.Background()

	leaseID := int64(123456789)
	s.Put([]byte("key1"), []byte("val1"), leaseID)
	s.Put([]byte("key2"), []byte("val2"), 0)
	s.Put([]byte("key3"), []byte("val3"), int64(987654321))

	// Test point query with KeysOnly
	res, err := s.Range(ctx, []byte("key1"), nil, RangeOptions{KeysOnly: true})
	if err != nil {
		t.Fatalf("Range failed: %v", err)
	}
	if len(res.KVs) != 1 {
		t.Fatalf("expected 1 kv, got %d", len(res.KVs))
	}
	if res.KVs[0].Lease != leaseID {
		t.Errorf("expected lease %d, got %d", leaseID, res.KVs[0].Lease)
	}
	if len(res.KVs[0].Value) != 0 {
		t.Errorf("expected empty value with KeysOnly, got %s", string(res.KVs[0].Value))
	}
	if string(res.KVs[0].Key) != "key1" {
		t.Errorf("expected key 'key1', got %s", string(res.KVs[0].Key))
	}

	// Test range / prefix query with KeysOnly
	res, err = s.Range(ctx, []byte("key"), []byte("key\xff"), RangeOptions{KeysOnly: true})
	if err != nil {
		t.Fatalf("Range prefix failed: %v", err)
	}
	if len(res.KVs) != 3 {
		t.Fatalf("expected 3 kvs, got %d", len(res.KVs))
	}

	expectedLeases := map[string]int64{
		"key1": leaseID,
		"key2": 0,
		"key3": 987654321,
	}

	for _, kv := range res.KVs {
		keyStr := string(kv.Key)
		expectedLease, ok := expectedLeases[keyStr]
		if !ok {
			t.Errorf("unexpected key %s", keyStr)
			continue
		}
		if kv.Lease != expectedLease {
			t.Errorf("key %s: expected lease %d, got %d", keyStr, expectedLease, kv.Lease)
		}
		if len(kv.Value) != 0 {
			t.Errorf("key %s: expected empty value with KeysOnly, got %s", keyStr, string(kv.Value))
		}
	}
}

func TestRangeFullMetadata(t *testing.T) {
	s := NewStore()
	defer s.Close()

	ctx := context.Background()
	leaseID := int64(42)

	rev1 := s.Put([]byte("meta-key"), []byte("initial-val"), leaseID)
	rev2 := s.Put([]byte("meta-key"), []byte("updated-val"), leaseID)

	res, err := s.Range(ctx, []byte("meta-key"), nil, RangeOptions{KeysOnly: true})
	if err != nil {
		t.Fatalf("Range failed: %v", err)
	}
	if len(res.KVs) != 1 {
		t.Fatalf("expected 1 kv, got %d", len(res.KVs))
	}
	kv := res.KVs[0]
	if kv.CreateRevision != rev1 {
		t.Errorf("expected CreateRevision %d, got %d", rev1, kv.CreateRevision)
	}
	if kv.ModRevision != rev2 {
		t.Errorf("expected ModRevision %d, got %d", rev2, kv.ModRevision)
	}
	if kv.Version != 2 {
		t.Errorf("expected Version 2, got %d", kv.Version)
	}
	if kv.Lease != leaseID {
		t.Errorf("expected Lease %d, got %d", leaseID, kv.Lease)
	}
	if len(kv.Value) != 0 {
		t.Errorf("expected empty Value, got %s", string(kv.Value))
	}
}
