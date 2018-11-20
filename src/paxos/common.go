package paxos

type instance_status struct {
	decided bool
	value interface{}
}

type instance_info struct {
	n_p int // highest proposal number seen so far by this paxos peer for this instance
	n_a int // highest porosal number has been accepted by this paxos peer for this instance
	v_a interface{} // the value of the instance with the highest accepted proposal number
	time_tried int
}


type PrepareArgs struct {
	Seq int
	Propose_n int
}
  
type PrepareReply struct {
	PrepareOK bool
	N_p int
	N_a int
	V_a interface{}
	//Seq int
}

type AcceptArgs struct {
	Seq int
	Propose_n int
	Value interface{}
}

type AcceptReply struct {
	AcceptOK bool
	//Seq int
	//Propose_n int
	N_done int
}

type DecideArgs struct {
	Seq int
	Propose_n int
	Value interface{}
}

type DecideReply struct {
	DecideOK bool
}
