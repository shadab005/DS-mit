package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	id          string
	currentTerm int
	votedFor    string
	log         []LogEntry

	//volatile on all servers
	commitIndex int
	lastApplied int

	//volatile on leaders and re-initialized after each election.
	nextIndex  []int
	matchIndex []int

	//Some more
	state         string //state will contain information if the server is LEADER, FOLLOWER, or CANDIDATE
	totalServers  int
	LastLogIndex  int
	LastLogTerm   int
	lastHeartBeat time.Time
}

const LEADER = "LEADER"
const FOLLOWER = "FOLLOWER"
const CANDIDATE = "CANDIDATE"
const NULL = "NULL"

type LogEntry struct {
	Term    int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == LEADER
}

func (rf *Raft) GetCurrentState() string {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.state
}

func (rf *Raft) GetCurrentTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm
}

func (rf *Raft) SetCurrentState(s string) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.state = s
}

func (rf *Raft) SetCurrentTerm(t int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.currentTerm = t
}

func (rf *Raft) GetLastHeartBeat() time.Time {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.lastHeartBeat
}

func (rf *Raft) SetLastHeartBeat(t time.Time) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.lastHeartBeat = t
}

func (rf *Raft) GetVotedFor() string {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.votedFor
}

func (rf *Raft) SetVotedFor(id string) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.votedFor = id
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
//
/*
Arguments:
term         => candidate’s term
candidateId  => candidate requesting vote
lastLogIndex => index of candidate’s last log entry (§5.4)
lastLogTerm  => term of candidate’s last log entry (§5.4)
*/
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  string
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
/*
Results:
term        => current Term, for candidate to update itself
voteGranted => true means candidate received vote
*/
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(request *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	reply.VoteGranted = false
	if request.Term < rf.GetCurrentTerm() {
		reply.VoteGranted = false
	} else {

		LogInfo("RequestVote: From %s and VotedFor = %s Checking if it can grant vote to %s with Term = %d",
			rf.ToString(), rf.GetVotedFor(), request.CandidateId, request.Term)
		if rf.GetVotedFor() == NULL || rf.GetVotedFor() == request.CandidateId {
			//checking if candidate's log entry is more up to date
			//if request.LastLogTerm > rf.LastLogTerm || (request.LastLogTerm == rf.LastLogTerm && request.LastLogIndex >= rf.LastLogIndex) {
			reply.VoteGranted = true
			rf.SetVotedFor(request.CandidateId)
			//}
		} else if request.Term >= rf.GetCurrentTerm() && rf.GetCurrentState() == CANDIDATE && rf.GetVotedFor() == rf.id {
			reply.VoteGranted = true
			rf.SetVotedFor(request.CandidateId)
			rf.SetCurrentState(FOLLOWER)
		}
	}
	reply.Term = rf.GetCurrentTerm()
	LogInfo("RequestVote: Receiver => %s ## Sender => [ Id : %s Term: %d VoteGranted? %v]", rf.ToString(),
		request.CandidateId, request.Term, reply.VoteGranted)

}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// AppendEntryArgs - RPC arguments
type AppendEntryArgs struct {
	Term         int
	LeaderID     string
	PrevLogIndex int
	PrevLogTerm  int
	LogEntries   []LogEntry
	LeaderCommit int
}

// AppendEntryReply - RPC response
type AppendEntryReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntry(request *AppendEntryArgs, reply *AppendEntryReply) {

	if request.Term < rf.GetCurrentTerm() {
		reply.Success = false
	} else if request.PrevLogIndex >= len(rf.log) || rf.log[request.PrevLogIndex].Term != request.Term {
		reply.Success = false
	} else {
		reply.Success = true
	}

	// TODO : If an existing entry conflicts with a new one, delete the existing entry and all that follow it
	// TODO : Append any new entry not in the log
	// TODO : If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)

	reply.Term = rf.GetCurrentTerm()

	//Receive request from valid leader. Return to follower state
	if request.Term >= rf.GetCurrentTerm() {
		//Become a follower
		rf.SetCurrentState(FOLLOWER)
		LogInfo("Setting VotedFor = NULL for %s", rf.id)
		rf.SetVotedFor(NULL)
		rf.SetCurrentTerm(request.Term)
		if len(request.LogEntries) == 0 {
			LogInfo("Setting Last HeartBeat for %s", rf.id)
			rf.SetLastHeartBeat(time.Now())
		}
	}

	LogInfo("AppendEntry: Receiver => %s ## Sender =>  [Leader Id : %s Term: %d]",
		rf.ToString(), request.LeaderID, request.Term)

}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.id = strconv.Itoa(me)
	rf.votedFor = NULL
	rf.state = FOLLOWER
	rf.totalServers = len(peers)
	rf.log = []LogEntry{}

	go rf.initElection()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) initElection() {

	for {
		for rf.GetCurrentState() == LEADER {
			time.Sleep(1 * time.Second)
		}
		//time.Sleep(150 * time.Millisecond)
		time.Sleep(time.Duration(200+rand.Intn(200)) * time.Millisecond)
		difference := time.Now().Sub(rf.GetLastHeartBeat())
		if difference >= 300*time.Millisecond {
			//start en election. reset the timeout
			LogInfo("Election Timed out by %v. New Election by %s", time.Now().Sub(rf.GetLastHeartBeat()), rf.ToString())
			rf.startElectionAsCandidate()
		}

	}

}


func (rf *Raft) startElectionAsCandidate() {

	//increment current Term
	rf.mu.Lock()
	rf.currentTerm++
	rf.mu.Unlock()
	//Transition to candidate
	rf.SetCurrentState(CANDIDATE)
	//Vote for itself
	rf.SetVotedFor(rf.id)

	voteChannel := make(chan bool)
	//invoke RequestVote rpcs in parallel to each server to get vote
	for i := 0; i < rf.totalServers; i++ {
		if i != rf.me {
			//Construct requestArg and invoke the api
			go func(j int) {
				//Get vote by calling the rpc
				requestVoteArgs := &RequestVoteArgs{rf.GetCurrentTerm(), rf.id, rf.LastLogIndex, rf.LastLogTerm}
				var requestVoteReply RequestVoteReply
				ok := rf.sendRequestVote(j, requestVoteArgs, &requestVoteReply)
				rf.mu.Lock()
				rf.currentTerm = max(rf.currentTerm, requestVoteReply.Term)
				rf.mu.Unlock()
				if ok {
					voteChannel <- requestVoteReply.VoteGranted
				} else {
					//fmt.Println("Request Vote RPC Failed")
					LogWarning("Request Vote Failed. Requester id = %s and requesting to = %d for Term = %d", rf.id, j, rf.GetCurrentTerm())
				}
			}(i)
		}
	}
	count := 1 //1 is for self vote

	select {
	case x := <-rf.countVotes(voteChannel):
		count = x
	case <-time.After(500 * time.Millisecond):
		LogWarning("[ID : %s Total Votes = %d Term = %d] Election couldn't select leader withing 0.5 seconds", rf.id, count, rf.GetCurrentTerm())
	}

	LogInfo("ID=%s And Vote Collected = %d ", rf.id, count)
	if count > rf.totalServers/2 && rf.GetCurrentState() == CANDIDATE {
		LogInfo("Elected Leader id %v from term = %d", rf.id, rf.GetCurrentTerm())
		rf.SetCurrentState(LEADER)
		go rf.sendHeartBeats()
	}

	//server din't receive enough votes
	if count <= rf.totalServers/2 && rf.GetCurrentState() != FOLLOWER {
		LogInfo("Server with id %s din't receive enough votes (Count = %d) for Term %d", rf.id, count, rf.GetCurrentTerm())
		rf.SetCurrentState(FOLLOWER)
	}

	LogInfo("%s SetVotedFor = NULL ", rf.ToString())
	rf.SetVotedFor(NULL)
}

func (rf *Raft) countVotes(voteChannel chan bool) chan int {
	countChannel := make(chan int)

	go func() {
		count := 1
		for i := 0; i < rf.totalServers; i++ {
			if i != rf.me {
				if rf.GetCurrentState() != CANDIDATE {
					return
				}
				vote, open := <-voteChannel
				if vote && open {
					count++
					if count > rf.totalServers/2 {
						countChannel <- count
						return
					}
				}
			}
		}
	}()

	return countChannel
}

func (rf *Raft) sendHeartBeats() {
	//The tester requires that the leader send heartbeat RPCs no more than ten times per second.
	// This means sleep for 100ms and then send heart beats to all server
	LogInfo("Sending HeartBeats. Leader = %s and State = %s", rf.id, rf.state)
	sendHeartBeatsToAllInParallel := func(leaderIndex int) {
		for i := 0; i < rf.totalServers && rf.GetCurrentState() == LEADER; i++ {
			if i != leaderIndex {

				go func(serverIndex int) {
					cTerm := rf.GetCurrentTerm()
					appendEntryArgs := &AppendEntryArgs{Term: cTerm, LeaderID: rf.id, LogEntries: []LogEntry{}}
					var appendEntryReply AppendEntryReply
					LogInfo("Sending HeartBeat to server index = %d from server = %s", serverIndex, rf.id)
					ok := rf.sendAppendEntry(serverIndex, appendEntryArgs, &appendEntryReply)
					//response from server has term greater. So leader should step down
					if appendEntryReply.Term > cTerm && rf.GetCurrentState() == LEADER {
						LogInfo("Demoting the Leader %s to Follower", rf.id)
						rf.SetCurrentState(FOLLOWER)
					}
					if rf.GetCurrentState() == FOLLOWER {
						return
					}
					if !ok {
						LogWarning("HeartBeat not sent to serverIndex %d from server = %s", serverIndex, rf.id)

					}
				}(i)

			}
		}
	}
	for rf.GetCurrentState() == LEADER {
		sendHeartBeatsToAllInParallel(rf.me)
		time.Sleep(100 * time.Millisecond)
	}

}

func (rf *Raft) ToString() string {
	return fmt.Sprintf("[ID: %s, State: %s, Term: %d]", rf.id, rf.GetCurrentState(), rf.GetCurrentTerm())
}
func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
