package v3rpc

import (
	"context"
	"testing"

	"github.com/alcaldoalgenus/etcd/server/storage/mvcc"
)

func TestKVServerRangeKeysOnlyPreservesLease(t *testing.T) {
	store := mvcc.NewStore()
	defer store.Close()

	leaseID := int64(88888)
	store.Put([]byte("foo"), []byte("bar"), leaseID)

	kvServer := NewKVServer(store)
	ctx := context.Background()

	resp, err := kvServer.Range(ctx, &RangeRequest{
		Key:      []byte("foo"),
		KeysOnly: true,
	})
	if err != nil {
		t.Fatalf("Range request failed: %v", err)
	}
	if len(resp.Kvs) != 1 {
		t.Fatalf("expected 1 kv in response, got %d", len(resp.Kvs))
	}
	if resp.Kvs[0].Lease != leaseID {
		t.Errorf("expected lease %d, got %d", leaseID, resp.Kvs[0].Lease)
	}
	if len(resp.Kvs[0].Value) != 0 {
		t.Errorf("expected empty value, got %s", string(resp.Kvs[0].Value))
	}
}
