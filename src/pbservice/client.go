package pbservice

import "viewservice"
import "net/rpc"
import "fmt"

// You'll probably need to uncomment these:
// import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
  vs *viewservice.Clerk
  view *viewservice.View
  me string
  // Your declarations here
}

func nrand() int64 {
 max := big.NewInt(int64(1) << 62)
 bigx, _ := rand.Int(rand.Reader, max)
 x := bigx.Int64()
 return x
}

func MakeClerk(vshost string, me string) *Clerk {
  ck := new(Clerk)
  ck.me = me
  ck.vs = viewservice.MakeClerk(me, vshost)

  ck.view = new(viewservice.View)
  // Your ck.* initializations here

  return ck
}


//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
          args interface{}, reply interface{}) bool {
  c, errx := rpc.Dial("unix", srv)
  if errx != nil {
    return false
  }
  defer c.Close()
    
  err := c.Call(rpcname, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}

//
// fetch a key's value from the current primary;
// if the key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {

  id := nrand()

  args := &GetArgs{}
  args.Key = key
  args.Id = id
  args.Sender = ck.me
  var reply GetReply

  view, _ := ck.vs.Get()
  ck.view = &view

  primary := ck.view.Primary

  for primary == "" {
    view, _ := ck.vs.Get()
    ck.view = &view
    primary = ck.view.Primary   
  }


  ok := call(primary, "PBServer.Get", args, &reply)

  // keep trying until primary replies
  for !ok || reply.Err == ErrWrongServer {
    view, _ := ck.vs.Get()
    ck.view = &view
    primary = ck.view.Primary 
    ok = call(primary, "PBServer.Get", args, &reply)
  }

  // if the key has never been set, return ""
  if reply.Err == ErrNoKey {
    return ""
  }

  return reply.Value
  // Your code here.

}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) PutExt(key string, value string, dohash bool) string {
  id := nrand()

  args := &PutArgs{}
  args.Id = id
  args.Key = key
  args.Value = value
  args.DoHash = dohash
  args.Sender = ck.me
  args.Forward = false

  var reply PutReply

  primary := ck.view.Primary
  for primary == "" {
    view, _ := ck.vs.Get()
    ck.view = &view
    primary = ck.view.Primary   
  }

  ok := call(primary, "PBServer.Put", args, &reply)

  for !ok || reply.Err == ErrWrongServer{
    view, _ := ck.vs.Get()
    ck.view = &view
    primary = ck.view.Primary

    ok = call(primary, "PBServer.Put", args, &reply)
  }
  // Your code here.

  return reply.PreviousValue
}

func (ck *Clerk) Put(key string, value string) {
  ck.PutExt(key, value, false)
}
func (ck *Clerk) PutHash(key string, value string) string {

  v := ck.PutExt(key, value, true)
  return v
}
