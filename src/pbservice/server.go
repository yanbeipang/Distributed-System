package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "os"
import "syscall"
import "math/rand"
import "sync"

import "strconv"


// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    n, err = fmt.Printf(format, a...)
  }
  return
}

type PBServer struct {
  l net.Listener
  dead bool // for testing
  unreliable bool // for testing
  me string
  vs *viewservice.Clerk
  done sync.WaitGroup
  finish chan interface{}
  db map[string]string
  log map[int64][]string
  mu sync.Mutex
  rwmu sync.RWMutex
  view *viewservice.View
  // Your declarations here.
}

func (pb *PBServer) HandlePut(args *PutArgs, reply *PutReply) error {

  
  if args.DoHash == true {

    pb.mu.Lock()
    //fmt.Printf("%#v\n", args)
    previousValue, ok := pb.db[args.Key]

    if !ok {
      previousValue = ""
    }

    value := hash(previousValue + args.Value)
    newValue := strconv.Itoa(int(value))
    pb.db[args.Key] = newValue
    reply.PreviousValue = previousValue
    pb.log[args.Id] = append(pb.log[args.Id], args.Key, previousValue)
    pb.mu.Unlock()


  } else {
    pb.mu.Lock()
    pb.db[args.Key] = args.Value
    pb.log[args.Id] = append(pb.log[args.Id], args.Key, args.Value)
    pb.mu.Unlock()

  }

  return nil
  
}

func (pb *PBServer) ForwardPut(forward_args *PutArgs, forward_reply *PutReply) error {
  //pb.mu.Lock()
  args := &PutArgs{}

  args.Id = forward_args.Id
  args.Key = forward_args.Key
  args.Value = forward_args.Value
  args.DoHash = forward_args.DoHash
  args.Sender = pb.me
  args.Forward = true

  ok := false

  for !ok && pb.view.Backup != "" {
    ok = call(pb.view.Backup, "PBServer.Put", args, &forward_reply)
  }
  //pb.mu.Unlock()

  return nil

}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {

  //pb.mu.Lock()
  //defer pb.mu.Unlock()
  // Your code here.
  
  // if it is the same put as before, do nothing and return the same result
  pb.rwmu.Lock()
  defer pb.rwmu.Unlock()

  if val, ok := pb.log[args.Id]; ok {

    reply.PreviousValue = val[1]

    return nil
    
  }

  // commit: unlock()
  // if the server is primary
  if pb.view.Primary == pb.me {

    //if the server is primary and request is forwarded, should reject
    if args.Forward == true {

      reply.Err = ErrWrongServer
      return nil
    }
    // deal with the put request, and forward request to backup (if there is any)
    // after receive done reply from backup, send commit to backup, do commit
    // reply to the client
    if pb.view.Backup != "" {

      var forward_reply PutReply
      // forwardPut will keep trying untill success
      pb.ForwardPut(args, &forward_reply)

      // if backup server replies that it's not backup, update view information, and try again
      if forward_reply.Err == ErrWrongServer {

        view, _ := pb.vs.Get()
        pb.view = &view
        return pb.Put(args, reply)
      }
    }
    // after successful forward, primary do put and commit

    pb.HandlePut(args, reply)

  } else {
    // if the server is not primary, it only handles RPC call from primary
    // if the call is not from primary, set replyerror
    // if the call is from primary, do put
    view, _ := pb.vs.Ping(pb.view.Viewnum)
    pb.view = &view

    if pb.view.Primary != pb.me && args.Sender != pb.view.Primary {
      reply.Err = ErrWrongServer
    } else {
      pb.HandlePut(args, reply)
    }
  }


  return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {


  // if the server is not primary
  time.Sleep(viewservice.PingInterval)
  if pb.view.Primary != pb.me {
    primary := pb.vs.Primary()
    if primary != pb.me {
      reply.Err = ErrWrongServer
      return nil
    } 
  }

  pb.mu.Lock()
  defer pb.mu.Unlock()
  value, ok := pb.db[args.Key]

  if !ok {
    reply.Err = ErrNoKey
    return nil
  }

  reply.Value = value
  
  // Your code here.
  return nil
}

func (pb *PBServer) SendDB(args *SendDBArgs, reply *SendDBReply) error {

    pb.mu.Lock()
    pb.db = args.DB

    pb.mu.Unlock()

  return nil

}

// ping the viewserver periodically.
func (pb *PBServer) tick() {

  view, _ := pb.vs.Ping(pb.view.Viewnum)


  // server is primary server, and there is new backup, send db to the backup 
  if view.Primary == pb.me && view.Backup != "" && view.Viewnum != pb.view.Viewnum {
  //if view.Primary == pb.me && view.Backup != "" && view.Viewnum != pb.view.Viewnum && view.Backup != pb.view.Backup {
    //fmt.Printf("I %v, DB is %v\n", pb.me, pb.db)
    //fmt.Printf("Send DB to: %v \n", view.Backup)

    args := &SendDBArgs{}
    args.DB = pb.db
    args.Sender = pb.me
    var reply SendDBReply

    call(view.Backup, "PBServer.SendDB", args, &reply)

  }

  pb.view = &view
  // Your code here.
}


// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
  pb.dead = true
  pb.l.Close()
}


func StartServer(vshost string, me string) *PBServer {
  pb := new(PBServer)
  pb.me = me
  pb.vs = viewservice.MakeClerk(me, vshost)
  pb.finish = make(chan interface{})
  pb.db = make(map[string]string)
  pb.log = make(map[int64][]string)
  pb.view = new(viewservice.View)
  // Your pb.* initializations here.

  rpcs := rpc.NewServer()
  rpcs.Register(pb)

  os.Remove(pb.me)
  l, e := net.Listen("unix", pb.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  pb.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for pb.dead == false {
      conn, err := pb.l.Accept()
      if err == nil && pb.dead == false {
        if pb.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if pb.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          pb.done.Add(1)
          go func() {
            rpcs.ServeConn(conn)
            pb.done.Done()
          }()
        } else {
          pb.done.Add(1)
          go func() {
            rpcs.ServeConn(conn)
            pb.done.Done()
          }()
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && pb.dead == false {
        fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
        pb.kill()
      }
    }
    DPrintf("%s: wait until all request are done\n", pb.me)
    pb.done.Wait() 
    // If you have an additional thread in your solution, you could
    // have it read to the finish channel to hear when to terminate.
    close(pb.finish)
  }()

  pb.done.Add(1)
  go func() {
    for pb.dead == false {
      pb.tick()
      time.Sleep(viewservice.PingInterval)
    }
    pb.done.Done()
  }()

  return pb
}
