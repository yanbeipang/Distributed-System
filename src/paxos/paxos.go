package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"
import "time"



type Paxos struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  peers []string
  me int // index into peers[]
  n_done int //the maximum seq instance that could be forgotten for this paxos peer
  peers_done []int //the Done value of all peers, index is the paxos peer
  instance_status map[int]*instance_status//keep status for each instance within the local paxos peer
                                            //key is the seq of instance, value including decided or not, value of the decision
  instance_info map[int]*instance_info //key is the seq of instance                                 
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
  c, err := rpc.Dial("unix", srv)
  if err != nil {
    err1 := err.(*net.OpError)
    if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
      fmt.Printf("paxos Dial() failed: %v\n", err1)
    }
    return false
  }
  defer c.Close()
    
  err = c.Call(name, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}

// Upon receive prepare() call, the paxos peer check if args.Propose_n >= n_p
// if so, set n_p = Propose_n, replies ok; o.w., replies not ok
func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error {

  px.mu.Lock()
  _, info_ok := px.instance_info[args.Seq]
  _, status_ok := px.instance_status[args.Seq]

  if !info_ok {
    var info instance_info
    info.n_p = 0
    info.n_a = -1
    px.instance_info[args.Seq] = &info
  }

  if !status_ok {
    var status instance_status
    status.decided = false
    px.instance_status[args.Seq] = &status
  }

  info := px.instance_info[args.Seq]

  if args.Propose_n >= info.n_p {
    info.n_p =  args.Propose_n
    reply.PrepareOK = true
  } else {
    reply.PrepareOK = false
  }
  px.mu.Unlock()

  reply.N_a = info.n_a
  reply.N_p = info.n_p
  reply.V_a = info.v_a
  
  return nil
}

// Upon receive accept() call, the paxos checks if args.Propose_n >= n_p
// if so, set n_p = Propose_n, n_a = Propose_n, v_a = args.Value, replies ok; o.w., replies not ok
func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error {

  px.mu.Lock()
  info := px.instance_info[args.Seq]

  if info == nil {
    info = &instance_info{}
    info.n_p = 0
    info.n_a = -1
  }
  if args.Propose_n >= info.n_p {
    info.n_p = args.Propose_n
    info.n_a = args.Propose_n
    info.v_a = args.Value

    reply.AcceptOK = true
  } else {
    reply.AcceptOK = false
  }
  px.mu.Unlock()
  reply.N_done = px.n_done
  
  return nil
}

// Upon receive decide(), the paxos update its instance_list for that instance with decide is true, value is the decided value
func (px *Paxos) Decide(args *DecideArgs, reply *DecideReply) error {
  px.mu.Lock()
  defer px.mu.Unlock()

  status := px.instance_status[args.Seq]
 
  if status == nil {
      status = &instance_status{}
  }
  status.decided = true
  status.value = args.Value
  reply.DecideOK = true

  return nil
}


//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//

// after the paxos peer receives the proposed instance from the application, it first calls prepare() to all paxos, and create 
// a list of replies; when wait for reply of other peers, should use for loop, and
// also need to check if the peer is alive or not
// check whether majorites of the replies are ok, and keep track of the v_a with the largest n_a

func (px *Paxos) StartPrepare(seq int, v interface{}) {

  decided := false
  for !decided && px.dead == false {
      
    px.mu.Lock()
    status, ok := px.instance_status[seq]
    info, info_ok := px.instance_info[seq]
    if ok && status.decided == true {
      px.mu.Unlock()
      return // if the instance already decided, ignore it
    }
    
    if !ok || status == nil { // if the instance not existed, add to the status map and info map
      status = &instance_status{}
      status.decided = false      
    }

    if !info_ok || info == nil {
      info = &instance_info{}
      info.n_p = 0
      info.n_a = -1
    }

    propose_n := info.n_p + px.me + 1

    info.n_p = propose_n
    px.mu.Unlock()

    args := &PrepareArgs{seq, propose_n}

    npaxos := len(px.peers)
    prepare_replies := make([]PrepareReply, npaxos)

    for i := 0; i < npaxos; i++ {
      var reply PrepareReply
      if i != px.me {
        call(px.peers[i], "Paxos.Prepare", args, &reply)

      } else if i == px.me{
        // if it's local paxos, make function call directly
          px.Prepare(args, &reply)
      }
      prepare_replies[i] = reply
    }

    // check if majorites reply ok, also keep track of the v_a with highest n_a, and update n_p to keep it as the highest 
    prepareOK_count := 0

    max_n_a := -1
    for i := 0; i < npaxos; i++ {
      reply := prepare_replies[i]
      if reply != (PrepareReply{}) {
        if max_n_a < reply.N_a {
          v = reply.V_a
          max_n_a = reply.N_a
        }
        if reply.PrepareOK == true {
          prepareOK_count ++
        } 
        px.mu.Lock()
        info := px.instance_info[seq]
 
        if info == nil {
          info = & instance_info{}
          info.n_p = 0
          info.n_a = -1
        }
        if info.n_p < reply.N_p {
          info.n_p = reply.N_p
        }
        px.mu.Unlock()
      }

    }

    if prepareOK_count >= (npaxos / 2) + 1 {
      px.StartAccept(seq, propose_n, v)
    }
    time.Sleep(time.Millisecond * 5)
    px.mu.Lock()
    status, ok = px.instance_status[seq]
    if ok {
      decided = status.decided
    }
    
    px.mu.Unlock()
    
  }

}

//majority is ok, send accept() to all paxos, create a list of replies; samely, using for loop to wait for replies;
func (px *Paxos) StartAccept(seq int, propose_n int, v interface{}) {
  
  args := &AcceptArgs{seq, propose_n, v}
  npaxos := len(px.peers)
  accept_replies := make([]AcceptReply, npaxos)

  for i := 0; i < npaxos; i++ {
    var reply AcceptReply
    if i != px.me {
      ok := false
      t := time.Now()
      // set time out for waiting reply
      for !ok && time.Since(t) < 10 * time.Millisecond && px.dead == false {
        ok = call(px.peers[i], "Paxos.Accept", args, &reply)
      }

    } else if i == px.me && px.dead == false {
      px.Accept(args, &reply)
    }
    accept_replies[i] = reply
  }

  // check if majorites reply ok, also update the done values for all peers
  acceptOK_count := 0
  for i := 0; i < npaxos; i++ {
    reply := accept_replies[i]
    if reply != (AcceptReply{}) {
      if reply.AcceptOK == true {
        acceptOK_count ++
      }
      px.peers_done[i] = reply.N_done
    }
  }
  if acceptOK_count >= (npaxos / 2) + 1 {
    px.StartDecide(seq, propose_n, v)
  } 
  
  return

}

// if accpet majority is ok, send decide() to all paxos
func (px *Paxos)StartDecide(seq int, propose_n int, v interface{}) {

  args := &DecideArgs{seq, propose_n, v}
  npaxos := len(px.peers)

  for i := 0; i < npaxos; i++ {
    var reply DecideReply

    if i != px.me {
      call(px.peers[i], "Paxos.Decide", args, &reply)
    } else if i == px.me && px.dead == false {
      // if it's local paxos, make function call directly
      px.Decide(args, &reply)
    }
  }

  return

}

// When an application calls for a new instance, check the seq number, if it's smaller than Min(), ignore it;
// then check if the instance already decided, if so, ignore too;
// o.w., create a new proposal number of the instance, which would be greater than this peer's n_p and include this peer's "me"
// create rountine for the new proposed instance to all paxos peers
// 
func (px *Paxos) Start(seq int, v interface{}) {
  time.Sleep(time.Millisecond * 5)
  // if seq is smaller than Min(), ignore it
  min := px.Min()
  if seq < min {
    return
  }
  
  go func(){
    px.StartPrepare(seq, v)
  }()

  return

}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation

// Upon receive Done(), the paxos update its n_done, and call Min() to get the minimum done seq num of all peers,
// discard all instance history with seq num < the minimum (delete from the map)
func (px *Paxos) Done(seq int) {
  
  px.n_done = seq
  px.peers_done[px.me] = seq

}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
  px.mu.Lock()
  defer px.mu.Unlock()
  max_seq := -1
  for k,_ := range px.instance_status {
    if k > max_seq {
      max_seq = k
    }
  }
  return max_seq
}

//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
// 
func (px *Paxos) Min() int {
  px.mu.Lock()
  defer px.mu.Unlock()
  min_propose_n := px.n_done
  for _,v := range px.peers_done {
    if v < min_propose_n {
      min_propose_n = v
    }
  }

  for k,_ := range px.instance_status {
    if k < min_propose_n + 1 {
      _, ok1 := px.instance_status[k]
      if ok1 {
        delete(px.instance_status, k)
      }
      _, ok2 := px.instance_info[k]
      if ok2 {
        delete(px.instance_info, k)
      }
      
    }
  }
  return min_propose_n + 1
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
  px.mu.Lock()
  defer px.mu.Unlock()
  decided := false
  var value interface{}
  s,ok := px.instance_status[seq]
  if ok {
    decided = s.decided
    value = s.value
  }
  return decided, value
}


//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
  px.dead = true
  if px.l != nil {
    px.l.Close()
  }
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
  px := &Paxos{}
  px.peers = peers
  px.me = me

  px.n_done = -1
  px.peers_done = make([]int, len(peers))
  px.instance_status = make(map[int]*instance_status)
  px.instance_info = make(map[int]*instance_info)


  if rpcs != nil {
    // caller will create socket &c
    rpcs.Register(px)
  } else {
    rpcs = rpc.NewServer()
    rpcs.Register(px)

    // prepare to receive connections from clients.
    // change "unix" to "tcp" to use over a network.
    os.Remove(peers[me]) // only needed for "unix"
    l, e := net.Listen("unix", peers[me]);
    if e != nil {
      log.Fatal("listen error: ", e);
    }
    px.l = l
    
    // please do not change any of the following code,
    // or do anything to subvert it.
    
    // create a thread to accept RPC connections
    go func() {
      for px.dead == false {
        conn, err := px.l.Accept()
        if err == nil && px.dead == false {
          if px.unreliable && (rand.Int63() % 1000) < 100 {
            // discard the request.
            conn.Close()
          } else if px.unreliable && (rand.Int63() % 1000) < 200 {
            // process the request but force discard of reply.
            c1 := conn.(*net.UnixConn)
            f, _ := c1.File()
            err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
            if err != nil {
              fmt.Printf("shutdown: %v\n", err)
            }
            px.rpcCount++
            go rpcs.ServeConn(conn)
          } else {
            px.rpcCount++
            go rpcs.ServeConn(conn)
          }
        } else if err == nil {
          conn.Close()
        }
        if err != nil && px.dead == false {
          fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
        }
      }
    }()
  }


  return px
}
