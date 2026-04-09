package raftapi

type Tversion int

type Err string

const (
	OK             Err = "OK"
	ErrNoKey       Err = "ErrNoKey"
	ErrWrongLeader Err = "ErrWrongLeader"
	ErrVersion     Err = "ErrVersion"
	ErrMaybe       Err = "ErrMaybe"
)

type GetArgs struct {
	Key string
}

type GetReply struct {
	Value   string
	Version Tversion
	Expires int64
	Err     Err
}

type PutArgs struct {
	Key     string
	Value   string
	Version Tversion
	TTL     int64 // seconds; <=0 means no expiry
}

type PutReply struct {
	Err      Err
	OldValue string // 修改前的值（用于Watch事件）
}

type DeleteArgs struct {
	Key string
}

type DeleteReply struct {
	Err      Err
	OldValue string
}

type ScanArgs struct {
	Prefix string
	Limit  int32
}

type ScanItem struct {
	Key     string
	Value   string
	Version Tversion
	Expires int64
}

type ScanReply struct {
	Items []ScanItem
	Err   Err
}

type ExpireArgs struct {
	Keys   []string
	Cutoff int64
}

type ExpireReply struct {
	ExpiredKeys      []string
	ExpiredOldValues map[string]string
	Err              Err
}
