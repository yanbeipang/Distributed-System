
package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type ViewServer struct {
  rwmu sync.RWMutex
  mu sync.Mutex
  l net.Listener
  dead bool
  me string

  server_time map[string]time.Time
  currentView *View
  currentPrimaryAcknowledge bool
  idleServer []string

}

//
// server Ping RPC handler.
//

func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
  
  server := args.Me
  vs.mu.Lock()
  vs.server_time[server] = time.Now()
  

  if vs.currentView.Viewnum == 0 {
    // initial (at the beginning)
    vs.currentView.Primary = server
    vs.currentPrimaryAcknowledge = false
    vs.currentView.Viewnum ++
  } else if vs.currentView.Primary == server {
    
    if vs.currentView.Viewnum == args.Viewnum {
      // Primary server acknowledge
      vs.currentPrimaryAcknowledge = true

    } else if args.Viewnum == 0 {
      // primary server restarted ===> primary server has died once
      //if vs.currentPrimaryAcknowledge == true && vs.currentView.Backup != "" {
      if vs.currentView.Backup != "" {
        vs.currentView.Primary = vs.currentView.Backup
        vs.currentPrimaryAcknowledge = false
        vs.currentView.Backup = ""
        vs.currentView.Viewnum ++
        // if there is idle server, promote it as backup server
        if len(vs.idleServer) != 0 {
          idle := vs.idleServer[0]
          vs.idleServer = vs.idleServer[1:]
          if idle != vs.currentView.Primary && idle != vs.currentView.Backup {
            vs.currentView.Backup = idle
          }

        }
      }
    }

  } else if vs.currentView.Primary != "" && vs.currentView.Backup == "" {
    // Add Backup server: current backup server is "" && current view's primary server has acknowledged
    if vs.currentPrimaryAcknowledge == true {
      vs.currentView.Backup = server
      vs.currentView.Viewnum ++
      vs.currentPrimaryAcknowledge = false
    }
  } else if vs.currentView.Primary != "" && vs.currentView.Backup != "" {
    // Add idle servers
    if server != vs.currentView.Backup && server != vs.currentView.Primary {
      serverInList := false
      for _,s := range vs.idleServer {
        if s==server {
          serverInList = true
          break
        }
      }
      if serverInList == false {
        vs.idleServer = append(vs.idleServer, server)
      }
    }
    
  }
  reply.View = *vs.currentView
  vs.mu.Unlock()
  return nil
}

// 
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {

  reply.View = *vs.currentView

  return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
  primary := vs.currentView.Primary
  backup := vs.currentView.Backup
  vs.mu.Lock()
  //Primary has failed && backup != "" -> promote the backup server as primary server
  if vs.currentPrimaryAcknowledge == true {
    if time.Now().Sub(vs.server_time[primary]) > DeadPings * PingInterval { 
      vs.currentView.Primary = ""    
    }
    //Backup has failed
    if time.Now().Sub(vs.server_time[backup]) > DeadPings * PingInterval {  
        vs.currentView.Backup = "" 
        vs.currentView.Viewnum ++
    }

    //Check whether there is failed idle server
    tmp := vs.idleServer[:0]
    for _, server := range vs.idleServer {
      if time.Now().Sub(vs.server_time[server]) <= DeadPings * PingInterval {
        tmp = append(tmp, server)
      }
    }
    vs.idleServer = tmp
  }

  if vs.currentView.Primary == "" && vs.currentPrimaryAcknowledge == true && vs.currentView.Backup != "" {
    vs.currentView.Primary = vs.currentView.Backup
    vs.currentPrimaryAcknowledge = false
    vs.currentView.Backup = ""
    vs.currentView.Viewnum ++
    // if there is idle server, promote it as backup server
    if len(vs.idleServer) != 0 {
      idle := vs.idleServer[0]
      vs.idleServer = vs.idleServer[1:]
      if idle != vs.currentView.Backup && idle != vs.currentView.Primary {
        vs.currentView.Backup = idle
      }
    }
  }
  //fmt.Printf("%#v",vs.currentView)
  vs.mu.Unlock()

  // Your code here.
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
  vs.dead = true
  vs.l.Close()
}

func StartServer(me string) *ViewServer {
  vs := new(ViewServer)
  vs.me = me
  vs.dead = false
  vs.server_time = make(map[string]time.Time)
  vs.currentView = new(View)
  vs.currentView.Viewnum = 0
  vs.currentPrimaryAcknowledge = false
  vs.idleServer = make([]string, 0)
  // Your vs.* initializations here.

  // tell net/rpc about our RPC server and handlers.
  rpcs := rpc.NewServer()
  rpcs.Register(vs)

  // prepare to receive connections from clients.
  // change "unix" to "tcp" to use over a network.
  os.Remove(vs.me) // only needed for "unix"
  l, e := net.Listen("unix", vs.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  vs.l = l

  // please don't change any of the following code,
  // or do anything to subvert it.

  // create a thread to accept RPC connections from clients.
  go func() {
    for vs.dead == false {
      conn, err := vs.l.Accept()
      if err == nil && vs.dead == false {
        go rpcs.ServeConn(conn)
      } else if err == nil {
        conn.Close()
      }
      if err != nil && vs.dead == false {
        fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
        vs.Kill()
      }
    }
  }()

  // create a thread to call tick() periodically.
  go func() {
    for vs.dead == false {
      vs.tick()
      time.Sleep(PingInterval)
    }
  }()

  return vs
}
