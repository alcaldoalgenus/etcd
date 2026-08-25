package mvcc

// KeyValue represents a key-value pair stored in MVCC storage.
type KeyValue struct {
	Key            []byte `protobuf:"bytes,1,opt,name=key,proto3" json:"key,omitempty"`
	CreateRevision int64  `protobuf:"varint,2,opt,name=create_revision,json=createRevision,proto3" json:"create_revision,omitempty"`
	ModRevision    int64  `protobuf:"varint,3,opt,name=mod_revision,json=modRevision,proto3" json:"mod_revision,omitempty"`
	Version        int64  `protobuf:"varint,4,opt,name=version,proto3" json:"version,omitempty"`
	Value          []byte `protobuf:"bytes,5,opt,name=value,proto3" json:"value,omitempty"`
	Lease          int64  `protobuf:"varint,6,opt,name=lease,proto3" json:"lease,omitempty"`
}

// RangeOptions provides parameters for range and point queries.
type RangeOptions struct {
	Limit        int64
	MinModRev    int64
	MaxModRev    int64
	MinCreateRev int64
	MaxCreateRev int64
	KeysOnly     bool
	CountOnly    bool
}

// RangeResult represents the output of a range query.
type RangeResult struct {
	KVs   []KeyValue
	Count int
	Rev   int64
	More  bool
}
