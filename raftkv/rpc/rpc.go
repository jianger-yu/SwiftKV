package rpc

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
	Err     Err
}

type PutArgs struct {
	Key     string
	Value   string
	Version Tversion
}

type PutReply struct {
	Err      Err
	OldValue string // 修改前的值（用于Watch事件）
}

type DeleteArgs struct {
	Key string
}

type DeleteReply struct {
	Err Err
}
