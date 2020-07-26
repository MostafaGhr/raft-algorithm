package main

import (
	"bufio"
	"log"
	"math"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

var timeOffset = 10

type RPCListener int

type Raft struct {
	mu sync.Mutex 			// Lock to protect shared access to this peer's state
	lastHeartbeat time.Time // Time for detecting leader time outs
	myID int				// my node ID
	nodesID []int			// all node IDs
	myState string			// node state
	currentTerm int			// current term for each node
	votedFor int			// node ID which this node has voted for
	commitIndex int  		// index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied int			// index of highest log entry applied to state machine (initialized to 0, increases monotonically)
	nextIndex map[string]int// for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int		// for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)
	log [100]Log			//log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1)
	lastLog int 			//index of last log
}

type Log struct {
	Command string
	Term int
}

type AppendEntriesArgs struct {
	Term int			//leader’s term
	LeaderId int		//so follower can redirect clients
	PrevLogIndex int	//index of log entry immediately preceding new ones
	PrevLogTerm int		//term of prevLogIndex entry
	Entries Log 		//log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int	//leader’s commitIndex
}
type AppendEntriesReply struct {
	Term int 			//currentTerm, for leader to update itself
	Success bool		//true if follower contained entry matching prevLogIndex and prevLogTerm
}
type RequestVoteArgs struct {
	Term int 			//candidate’s term
	CandidateId int 	//candidate requesting vote
	LastLogIndex int 	//index of candidate’s last log entry (§5.4)
	LastLogTerm int 	//term of candidate’s last log entry (§5.4)
}
type RequestVoteReply struct {
	Term int				//currentTerm, for candidate to update itself
	VoteGranted bool		//true means candidate received vote

}

func SendRequestVoteRPC(ID int, args *RequestVoteArgs, reply *RequestVoteReply){
	client, err := rpc.Dial("tcp", "localhost:" + strconv.Itoa(ID))
	if err != nil {
		println("no connection to", strconv.Itoa(ID), "could be made")
	} else {
		err = client.Call("RPCListener.RequestVote", &args , &reply)
		if err != nil {
			log.Fatal(err)
		}

	}
}

func SendAppendEntriesRPC(ID int, args *AppendEntriesArgs, reply *AppendEntriesReply){
	client, err := rpc.Dial("tcp", "localhost:" + strconv.Itoa(ID))
	if err != nil {
		println("no connection to", strconv.Itoa(ID), "could be made")
	} else {
		err = client.Call("RPCListener.AppendEntries", &args , &reply)
		if err != nil {
			log.Fatal(err)
		}

	}
}

func (l *RPCListener)AppendEntries(input *AppendEntriesArgs, output *AppendEntriesReply) error {
	rf.lastHeartbeat = time.Now()
	output.Term = rf.currentTerm
	//If leaderCommit > commitIndex, set commitIndex =
	//min(leaderCommit, index of last new entry)
	if input.LeaderCommit > rf.commitIndex {
		rf.commitIndex = int(math.Min(float64(input.LeaderCommit), float64(input.PrevLogIndex)))
	}
	if input.Term < rf.currentTerm {
		output.Success = false
		return nil
	}
	if input.Term > rf.currentTerm {
		rf.currentTerm = input.Term
		rf.myState = "follower"
		return nil
	}
	//if input.PrevLogTerm == rf.currentTerm && rf.log[input.PrevLogIndex].Command == "" {
	//
	//}
	if input.PrevLogTerm == rf.log[input.PrevLogIndex].Term && rf.log[input.PrevLogIndex].Term > 0 { //consistency check
		rf.mu.Lock()
		output.Success = true
		rf.mu.Unlock()
		if input.Entries.Command != "" && input.Entries.Term != -1 { // Append Log
			println("new log appended to", rf.myID)
			rf.log[input.PrevLogIndex + 1].Term = input.Entries.Term
			rf.log[input.PrevLogIndex + 1].Command = input.Entries.Command

		} else {	//Heartbeat

		}
	} else { // check for prev logs and solve inconsistency
		println("inconsistent log append")
		rf.mu.Lock()
		output.Success = false
		rf.mu.Unlock()
	}

	for i, txt := range rf.log{
		if txt.Command == ""{
			break
		}
		//println(i, txt.Term, txt.Command)
		rf.lastLog = i + 1
		println(i, txt.Term, txt.Command[:len(txt.Command)-1])
	}
	println("end of log")
	return nil
}

func (l *RPCListener)RequestVote(input *RequestVoteArgs, output *RequestVoteReply) error {
	output.Term = rf.currentTerm
	output.VoteGranted = false
	if input.Term > rf.currentTerm {
		rf.mu.Lock()
		rf.currentTerm = input.Term
		rf.myState = "follower"
		rf.mu.Unlock()
		return nil
	}
	if rf.votedFor == -1 || rf.votedFor == input.CandidateId{
		go func() {
			time.Sleep(time.Duration(600*timeOffset) * time.Millisecond)
			rf.votedFor = -1
		}()
		rf.mu.Lock()
		rf.votedFor = input.CandidateId
		output.VoteGranted = true
		rf.mu.Unlock()
	}
	return nil
}


var rf = Raft{
	mu:            	sync.Mutex{},
	lastHeartbeat: 	time.Now(),
	myID:           5000,
	nodesID:       	[]int{3000, 4000, 5000},
	myState:       	"follower",
	votedFor:	   	-1,
	currentTerm: 	0,
	commitIndex: 	0,
	lastApplied: 	0,
	log: 			[100]Log{{Command: "init\n", Term: 1}, {Command: "init\n", Term: 1}},
	nextIndex: 		map[string]int{"3000" : 2, "4000" : 2, "5000" : 2},
	lastLog: 		2,
}

func main() {
	println("serverID:", rf.myID)
	time.Sleep(time.Second)
	go rpcListenerSetup()

	go func() {
		for true {
			if rf.myState == "leader" {
				println("3000", rf.nextIndex["3000"], "4000", rf.nextIndex["4000"], "5000", rf.nextIndex["5000"])
				for _, ID := range rf.nodesID {
					var AppendReply = AppendEntriesReply{
						Term:    0,
						Success: false,
					}
					if rf.nextIndex[strconv.Itoa(rf.myID)] == rf.nextIndex[strconv.Itoa(ID)] {
						SendAppendEntriesRPC(ID, &AppendEntriesArgs{ // Heartbeat
							Term:         rf.currentTerm,
							LeaderId:     rf.myID,
							PrevLogIndex: rf.nextIndex[strconv.Itoa(ID)] - 1,
							PrevLogTerm:  rf.log[rf.nextIndex[strconv.Itoa(ID)]-1].Term,
							Entries:      Log{Command: "", Term: -1},
							LeaderCommit: rf.commitIndex,
						}, &AppendReply)
						if AppendReply.Success == false {
							rf.mu.Lock()
							rf.nextIndex[strconv.Itoa(ID)]--
							rf.mu.Unlock()

						}
					} else {
						var AppendReply = AppendEntriesReply{
							Term:    0,
							Success: false,
						}
						SendAppendEntriesRPC(ID, &AppendEntriesArgs{ // AppendLog
							Term:         rf.currentTerm,
							LeaderId:     rf.myID,
							PrevLogIndex: rf.nextIndex[strconv.Itoa(ID)] - 1,
							PrevLogTerm:  rf.log[rf.nextIndex[strconv.Itoa(ID)] - 1].Term,
							Entries: Log{Command: rf.log[rf.nextIndex[strconv.Itoa(ID)]].Command,
								Term: rf.log[rf.nextIndex[strconv.Itoa(ID)]].Term},
							LeaderCommit: rf.commitIndex,
						}, &AppendReply)
						if AppendReply.Success == true {
							rf.mu.Lock()
							rf.nextIndex[strconv.Itoa(ID)]++
							rf.mu.Unlock()
						}
						if AppendReply.Success == false {
							rf.mu.Lock()
							if rf.nextIndex[strconv.Itoa(ID)] > 2 {
								rf.nextIndex[strconv.Itoa(ID)]--
							}
							rf.mu.Unlock()
						}
					}

				}
				var commitCount = 0
				for _, ID := range rf.nodesID{
					if rf.commitIndex < rf.nextIndex[strconv.Itoa(ID)] - 1 {
						commitCount++
					}
				}
				if commitCount - 1 >= len(rf.nodesID) / 2{
					rf.commitIndex++
				}
			}
			time.Sleep(time.Duration((rand.Intn(300)+100)*timeOffset) * time.Millisecond)

		}
	}()

	go func() {
		for true {
			rf.mu.Lock()
			TTimer := time.Now().Sub(rf.lastHeartbeat).Milliseconds()
			rf.mu.Unlock()
			if TTimer > int64(1000*timeOffset) && rf.myState == "follower" { // rand.Intn(400) + 600 5println("leader timed out") //election
				rf.mu.Lock()
				rf.myState = "candidate"
				rf.mu.Unlock()
				var voteCounter = 0
				for _, ID := range rf.nodesID {
					func() {
						println("sending vote req to", ID, "from", rf.myID)
						var voteReply = RequestVoteReply{
							Term:        0,
							VoteGranted: false,
						}
						SendRequestVoteRPC(ID, &RequestVoteArgs{
							Term:         rf.currentTerm,
							CandidateId:  rf.myID,
							LastLogIndex: rf.nextIndex[strconv.Itoa(ID)] - 1,
							LastLogTerm:  rf.log[rf.nextIndex[strconv.Itoa(ID)] - 1].Term,
						}, &voteReply)

						println(ID, " voted for", rf.myID, voteReply.VoteGranted)

						if voteReply.VoteGranted {
							rf.mu.Lock()
							voteCounter++
							rf.mu.Unlock()
						}

					}()
				}
				println("vote count *", voteCounter)
				if voteCounter-1 >= len(rf.nodesID)/2 {
					println("elected as leader")
					rf.mu.Lock()
					voteCounter = 0
					rf.currentTerm++
					rf.myState = "leader"
					rf.mu.Unlock()
					for _, ID := range rf.nodesID {
						rf.nextIndex[strconv.Itoa(ID)] = rf.lastLog
						var AppendReply = AppendEntriesReply{
							Term:    0,
							Success: false,
						}
						func() {
							SendAppendEntriesRPC(ID, &AppendEntriesArgs{
								Term:         rf.currentTerm,
								LeaderId:     rf.myID,
								PrevLogIndex: rf.nextIndex[strconv.Itoa(ID)] - 1,
								PrevLogTerm:  rf.log[rf.nextIndex[strconv.Itoa(ID)] - 1].Term,
								Entries:      Log{Command: "", Term: -1},
								LeaderCommit: rf.commitIndex,
							}, &AppendReply)

							//if AppendReply.Success == true{
							//	rf.mu.Lock()
							//	rf.nextIndex[strconv.Itoa(ID)]++
							//	rf.mu.Unlock()
							//} else {
							//	rf.mu.Lock()
							//	rf.nextIndex[strconv.Itoa(ID)]--
							//	rf.mu.Unlock()
							//}
						}()
					}
				} else {
					rf.mu.Lock()
					rf.myState = "follower"
					voteCounter = 0
					rf.mu.Unlock()
				}
			}

			time.Sleep(time.Duration(10*timeOffset) * time.Millisecond)
		}
	}()

	func() {
		for true {
			if rf.myState == "leader" {
				println("type your log")
				reader := bufio.NewReader(os.Stdin)
				input, _ := reader.ReadString('\n')

				rf.log[rf.nextIndex[strconv.Itoa(rf.myID)]].Term = rf.currentTerm
				rf.log[rf.nextIndex[strconv.Itoa(rf.myID)]].Command = input
				rf.mu.Lock()
				rf.nextIndex[strconv.Itoa(rf.myID)]++
				rf.mu.Unlock()

				//for _, ID := range rf.nodesID {
				//	func() {
				//		var AppendReply = AppendEntriesReply{
				//			Term:    0,
				//			Success: false,
				//		}
				//		SendAppendEntriesRPC(ID, &AppendEntriesArgs{
				//			Term:         rf.currentTerm,
				//			LeaderId:     rf.myID,
				//			PrevLogIndex: rf.nextIndex[strconv.Itoa(ID)] - 1,
				//			PrevLogTerm:  rf.log[rf.nextIndex[strconv.Itoa(ID)] - 1].Term,
				//			Entries:      Log{Command: input, Term: rf.currentTerm},
				//			LeaderCommit: rf.commitIndex,
				//		}, &AppendReply)
				//
				//		if AppendReply.Success == true {
				//			rf.mu.Lock()
				//			rf.nextIndex[strconv.Itoa(ID)]++
				//			rf.mu.Unlock()
				//		}
				//	}()
				//}
			}
			time.Sleep(time.Duration(10*timeOffset) * time.Millisecond)
		}
	}()
}


func rpcListenerSetup(){
	netResolver, err := net.ResolveTCPAddr("tcp", "127.0.0.1:" + strconv.Itoa(rf.myID))
	if err != nil {
		log.Fatal(err)
	} else {
		netListener, err := net.ListenTCP("tcp", netResolver)
		if err != nil {
			log.Fatal(err)
		} else {
			listener := new(RPCListener)
			rpc.Register(listener)
			rpc.Accept(netListener)
		}
	}
}