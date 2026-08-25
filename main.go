package main

import (
	"context"
	"fmt"

	"github.com/alcaldoalgenus/etcd/server/storage/mvcc"
)

func main() {
	s := mvcc.NewStore()
	defer s.Close()

	leaseID := int64(1001)
	s.Put([]byte("sample-key"), []byte("sample-value"), leaseID)

	res, err := s.Range(context.Background(), []byte("sample-key"), nil, mvcc.RangeOptions{KeysOnly: true})
	if err != nil {
		panic(err)
	}

	if len(res.KVs) > 0 {
		fmt.Printf("Key: %s, Lease: %d, Value: %s\n", string(res.KVs[0].Key), res.KVs[0].Lease, string(res.KVs[0].Value))
	}
}
