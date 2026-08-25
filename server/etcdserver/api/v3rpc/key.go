package v3rpc

import (
	"context"

	"github.com/alcaldoalgenus/etcd/server/storage/mvcc"
)

// RangeRequest encapsulates parameters for a range RPC request.
type RangeRequest struct {
	Key               []byte
	RangeEnd          []byte
	Limit             int64
	Revision          int64
	MinModRevision    int64
	MaxModRevision    int64
	MinCreateRevision int64
	MaxCreateRevision int64
	KeysOnly          bool
	CountOnly         bool
}

// RangeResponse encapsulates the response for a range RPC request.
type RangeResponse struct {
	Header *ResponseHeader
	Kvs    []*mvcc.KeyValue
	More   bool
	Count  int64
}

// ResponseHeader contains general metadata about the response.
type ResponseHeader struct {
	Revision int64
}

// KVServer handles key-value RPC requests.
type KVServer struct {
	store mvcc.Store
}

// NewKVServer constructs a KVServer.
func NewKVServer(store mvcc.Store) *KVServer {
	return &KVServer{store: store}
}

// Range handles gRPC range requests.
func (s *KVServer) Range(ctx context.Context, req *RangeRequest) (*RangeResponse, error) {
	ro := mvcc.RangeOptions{
		Limit:        req.Limit,
		MinModRev:    req.MinModRevision,
		MaxModRev:    req.MaxModRevision,
		MinCreateRev: req.MinCreateRevision,
		MaxCreateRev: req.MaxCreateRevision,
		KeysOnly:     req.KeysOnly,
		CountOnly:    req.CountOnly,
	}

	var tr mvcc.TxnRead
	if req.Revision > 0 {
		tr = s.store.Read(req.Revision)
	} else {
		tr = s.store.Read(0)
	}
	defer tr.End()

	res, err := tr.Range(ctx, req.Key, req.RangeEnd, ro)
	if err != nil {
		return nil, err
	}

	kvs := make([]*mvcc.KeyValue, len(res.KVs))
	for i := range res.KVs {
		kvs[i] = &res.KVs[i]
	}

	return &RangeResponse{
		Header: &ResponseHeader{Revision: res.Rev},
		Kvs:    kvs,
		More:   res.More,
		Count:  int64(res.Count),
	}, nil
}
