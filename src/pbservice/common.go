package pbservice

import "hash/fnv"

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ErrWrongServer = "ErrWrongServer"
)
type Err string

type SendDBArgs struct {
  Sender string
  DB map[string]string
}

type SendDBReply struct {
  Err Err
}

type PutArgs struct {
  Key string
  Value string
  DoHash bool // For PutHash
  Id int64
  Sender string
  Forward bool
  // You'll have to add definitions here.

  // Field names must start with capital letters,
  // otherwise RPC will break.
}

type PutReply struct {
  Err Err
  PreviousValue string // For PutHash
}

type GetArgs struct {
  Key string
  Id int64
  Sender string
  // You'll have to add definitions here.
}

type GetReply struct {
  Err Err
  Value string
}


// Your RPC definitions here.

func hash(s string) uint32 {
  h := fnv.New32a()
  h.Write([]byte(s))
  return h.Sum32()
}

