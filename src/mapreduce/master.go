package mapreduce
import "container/list"
import "fmt"
import "sync"



type WorkerInfo struct {
  address string

}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
  l := list.New()
  for _, w := range mr.Workers {
    DPrintf("DoWork: shutdown %s\n", w.address)
    args := &ShutdownArgs{}
    var reply ShutdownReply;
    ok := call(w.address, "Worker.Shutdown", args, &reply)
    if ok == false {
      fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
    } else {
      l.PushBack(reply.Njobs)
    }
  }
  return l
}

func (mr *MapReduce) schedule(Operation JobType) {

    var jobsn int
    var otherjobsn int

    switch Operation {
    case Map:
      jobsn = mr.nMap
      otherjobsn = mr.nReduce
    case Reduce:
      jobsn = mr.nReduce
      otherjobsn = mr.nMap
    }

    var wg sync.WaitGroup

    var jobChan = make(chan int)

    //set job channel and waitgroup
    go func(){
        for i := 0; i < jobsn; i++ {
            wg.Add(1)
            jobChan <- i
        }
        wg.Wait()
        close(jobChan)
    }();

    //let all jobs to be done
    for i := range jobChan {
        worker := <- mr.registerChannel
        _, ok := mr.Workers[worker]
        if !ok {
          wi := new(WorkerInfo)
          wi.address = worker 
          mr.Workers[worker] = wi   
        }

        var args DoJobArgs

        var res DoJobReply
    
        args.File = mr.file
        args.Operation = Operation
        args.NumOtherPhase = otherjobsn
        args.JobNumber = i

        go func(worker string, args *DoJobArgs, res *DoJobReply) {
          ok := call(worker, "Worker.DoJob", args, res)
          if ok == true {
            wg.Done()
            mr.registerChannel <- worker
          }else {
            jobChan <- args.JobNumber
          } 
        }(worker, &args, &res);

    }

    fmt.Printf("schedule: %s done\n", Operation)


}

func (mr *MapReduce) RunMaster() *list.List {

  mr.schedule(Map)
  mr.schedule(Reduce)
  return mr.KillWorkers()
}
