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
	"bytes"
	"fmt"
	"labgob"
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
	mu             sync.Mutex // Lock to protect shared access to this peer's state
	peers          []*labrpc.ClientEnd // RPC end points of all peers
	persister      *Persister          // Object to hold this peer's persisted state
	me             int                 // this peer's index into peers[]

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
	lastHeartBeat time.Time
	applyCh       chan ApplyMsg
	//messageCh     chan int //channel that appends client request so that one by one can be fetched
	isAlive       *AtomicBool
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

func (rf *Raft) GetCommittedIndex() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.commitIndex
}

func (rf *Raft) SetCommittedIndex(index int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.commitIndex = index
}

// Returns last index of the log and its term
func (rf *Raft) getLastEntryInfo() (int, int) {
	if len(rf.log) > 0 {
		return len(rf.log) - 1, rf.log[len(rf.log)-1].Term
	}
	return 0, 0
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	_ = e.Encode(rf.currentTerm)
	_ = e.Encode(rf.votedFor)
	_ = e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
	LogWarning("[readPersist] Data = nil for %s", rf.id)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor string
	var log []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		LogWarning("Error in decoding %v %v and %v", currentTerm, votedFor, log)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		LogWarning("[readPersist] rf.id = %s rf.currentTerm = %d rf.votedFor = %s rf.log = %v", rf.id, rf.currentTerm, rf.votedFor, rf.log)
	}

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
// Must ensure Leader completeness property.
// This is done by checking if the candidate vote is more up-to-date than voter. Only then grant vote.
//
func (rf *Raft) RequestVote(request *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A, 2B).
	LogInfo("[RequestVote] \nServer : %s VotedFor = %s | Requester : CandidateId = %s Term = %d LastLogIndex = %d LastLogTerm = %d",
		rf.stringify(), rf.votedFor, request.CandidateId, request.Term, request.LastLogIndex, request.LastLogTerm)

	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	if request.Term < rf.currentTerm {
		LogInfo("[RequestVote] Server %s Requester %s | CandidateId is outdated", rf.id, request.CandidateId)
		reply.VoteGranted = false
		return
	}

	if request.Term > rf.currentTerm {
		rf.currentTerm = request.Term
		rf.votedFor = NULL
		rf.state = FOLLOWER
		LogInfo("[RequestVote] Server %s Requester %s | Updating Term = %d", rf.id, request.CandidateId, rf.currentTerm)
	}

	if rf.votedFor == NULL || rf.votedFor == request.CandidateId {
		//checking if candidate's log entry is more up to date
		lastLogIndex, lastLogTerm := rf.getLastEntryInfo()
		LogInfo("[RequestVote] Server %s Requester %s | LastLogIndex = %d and lastLogTerm = %d", rf.id, request.CandidateId, lastLogIndex, lastLogTerm)
		if request.LastLogTerm > lastLogTerm || (request.LastLogTerm == lastLogTerm && request.LastLogIndex >= lastLogIndex) {
			LogInfo("%s Granting vote to %s with term %d", rf.stringify(), request.CandidateId, request.Term)
			LogInfo("[RequestVote] Server %s Requester %s | Vote Granted", rf.id, request.CandidateId)
			reply.VoteGranted = true
			/*
				ISSUE
				A node can vote for two servers.
				Ex - Scenario 1 : check rf.GetVoteFor is null. Vote for self after election timeout and then vote for the candidate for which we just checked
				     if the voteGranted is null. So one server just voted for itself and another candidate.
				     Scenario 2 : Server votes for requesting candidate. Server election timeout then start as candidate and vote for itself.
				     Seems like we need to use mechanism like synchronization over the whole method or atomic variable.
			*/
			//resetting the election timeout when one grants vote.
			rf.lastHeartBeat = time.Now()
			rf.votedFor = request.CandidateId
			rf.state = FOLLOWER
			rf.currentTerm = request.Term
		}
	}
	rf.persist()

	LogInfo("[RequestVote] Server %s Requester %s | Exiting VoteGranted = %v", rf.id, request.CandidateId, reply.VoteGranted)
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

/*
   Should take care of normal case, missing entry, extraneous entry.
   Consider cases of partitioned leader and partitioned follower in minority
*/
func (rf *Raft) AppendEntry(request *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	LogInfo("[AppendEntry] \nServer : %s Commit Index = %d Log entry = %v \nRequester : LeaderID = %s term = %d PrevLogIndex = %d PrevLogTerm = %d Log = %v LeaderCommit = %d",
		rf.stringify(), rf.commitIndex, rf.log, request.LeaderID, request.Term, request.PrevLogIndex, request.PrevLogTerm, request.LogEntries, request.LeaderCommit)

	reply.Term = rf.currentTerm
	reply.Success = true

	//Server is more upto date. Leader is invalid
	if request.Term < rf.currentTerm {
		LogInfo("[AppendEntry] Server %s Requester %s | Leader is outdated", rf.id, request.LeaderID)
		reply.Success = false
		return
	}
	//Leader is valid
	//Log at previous index of leader is not matching with this server
	if request.PrevLogIndex >= len(rf.log) || rf.log[request.PrevLogIndex].Term != request.PrevLogTerm {
		//This is conflicting case
		LogInfo("[AppendEntry] Server %s Requester %s | log is outdated and conflicted", rf.id, request.LeaderID)
		reply.Success = false
	} else if reply.Success {
		LogInfo("[AppendEntry] Server %s Requester %s | Good case", rf.id, request.LeaderID)
		if len(request.LogEntries) != 0 { //Contains some new entry and is not heartbeat
			i := 0
			j := request.PrevLogIndex + 1
			conflict := false
			for j < len(rf.log) && i < len(request.LogEntries) && !conflict {
				//if conflict. Delete the existing entry and following it.
				if rf.log[j].Term != request.LogEntries[i].Term {
					conflict = true
				} else {
					j++
					i++
				}
			}
			if conflict {
				rf.log = rf.log[:j]
				rf.log = append(rf.log, request.LogEntries[i:]...)
			} else {
				if j >= len(rf.log) {
					if i < len(request.LogEntries) {
						rf.log = append(rf.log, request.LogEntries[i:]...)
					}
				} else if j < len(rf.log) {
					//j already had all entries of i and shouldn't be done anything in this case
				}
			}
			LogInfo("[AppendEntry] Server %s Requester %s | Server log = %v", rf.id, request.LeaderID, rf.log)
		}

		if reply.Success && request.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(request.LeaderCommit, len(rf.log)-1)
			LogInfo("[AppendEntry] Server %s Requester %s | Updated commit index = %d", rf.id, request.LeaderID, rf.commitIndex)
		}
		rf.state = FOLLOWER
	}

	if request.Term > rf.currentTerm {
		rf.currentTerm = request.Term
		rf.state = FOLLOWER
		LogInfo("[AppendEntry] Server %s Requester %s | Updated server term = %d and state = FOLLOWER", rf.id, request.LeaderID, rf.currentTerm)
	}
	rf.votedFor = NULL
	rf.lastHeartBeat = time.Now()
	rf.persist()
	LogInfo("[AppendEntry] Server %s Requester %s | Exiting by setting lastHeartBeat and resetting votedFor", rf.id, request.LeaderID)
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := rf.state == LEADER
	LogInfo("[**MESSAGE**] Command received from client %v for %s and isLeader = %v", command, rf.id, isLeader)
	// Your code here (2B).
	if isLeader {
		index = len(rf.log) //Index in the leader log where the new entry is going to be appended.
		term = rf.currentTerm
		rf.log = append(rf.log, LogEntry{term, command})
		rf.persist()
	}
	return index, term, isLeader
}

func (rf *Raft) handleClientMessages() {
	LogInfo("Handling client message channel by server %s", rf.id)
	for rf.GetCurrentState() == LEADER && rf.isAlive.Get() {
		rf.broadCastCommand()
		<-time.After(50 * time.Millisecond)
	}
	LogInfo("Message Handling done by the leader %s", rf.id)
}

func (rf *Raft) broadCastCommand() {
	LogInfo("[----------Message broadcasting----------] by %s ", rf.id)

	sendLogMessage := func() {
		for i := 0; i < rf.totalServers && rf.GetCurrentState() == LEADER && rf.isAlive.Get(); i++ {
			if i != rf.me {
				go func(j int) {

					prevIndex := 0
					prevTerm := 0
					nextIn := 0
					var entries []LogEntry

					if rf.GetCurrentState() == LEADER && rf.isAlive.Get(){

						rf.mu.Lock()
						flag := false
						prevIndex = rf.nextIndex[j] - 1
						if prevIndex < len(rf.log) {
							//needed flag because log was going out of index on prevIndex
							prevTerm = rf.log[prevIndex].Term
							nextIn = rf.nextIndex[j]
							entries = rf.log[nextIn:]
							flag = true
						}
						committedIndex := rf.commitIndex
						lastIndex, lastLogTerm := rf.getLastEntryInfo()
						currentTerm := rf.currentTerm

						rf.mu.Unlock()

						if flag && lastIndex >= nextIn {
							appendEntryArgs :=
								&AppendEntryArgs{
									Term:         currentTerm,
									LeaderID:     rf.id,
									LogEntries:   entries,
									LeaderCommit: committedIndex,
									PrevLogIndex: prevIndex,
									PrevLogTerm:  prevTerm}

							var appendEntryReply AppendEntryReply
							LogInfo("[Broadcast] Server %s sent AppendEntry RPC to Server %d", rf.id, j)
							ok := rf.sendAppendEntry(j, appendEntryArgs, &appendEntryReply)
							LogInfo("[Broadcast] Server %s received from Server %d Result = %v and ok = %v", rf.id, j, appendEntryReply, ok)
							if ok {
								if appendEntryReply.Success {
									//If successful: update nextIndex and matchIndex for follower (§5.3)
									rf.mu.Lock()
									rf.nextIndex[j] += len(entries)
									rf.matchIndex[j] = rf.nextIndex[j]-1
									rf.mu.Unlock()
								} else if currentTerm == rf.GetCurrentTerm() {
									//Case when reply fails. Reply could fail due to less term number or due to log inconsistency
									if appendEntryReply.Term > currentTerm {
										// promote to follower rf.promoteToFollower()
										rf.mu.Lock()
										rf.state = FOLLOWER
										rf.votedFor = NULL
										rf.currentTerm = appendEntryReply.Term
										rf.persist()
										rf.mu.Unlock()
									} else {
										rf.mu.Lock()
										rf.nextIndex[j]--
										rf.mu.Unlock()
									}
								}
							}

						} else {
							//send heart beat
							appendEntryArgs :=
								&AppendEntryArgs{Term: currentTerm,
									LeaderID:     rf.id,
									LogEntries:   []LogEntry{},
									LeaderCommit: committedIndex,
									PrevLogIndex: lastIndex,
									PrevLogTerm:  lastLogTerm}
							var appendEntryReply AppendEntryReply
							LogInfo("Sending HeartBeat to server index = %d from server = %s", j, rf.id)
							ok := rf.sendAppendEntry(j, appendEntryArgs, &appendEntryReply)
							LogInfo("HearBeat Reply from server %v", ok)
							//response from server has term greater. So leader should step down
							// cTerm == rf.GetCurrentTerm() to check if it wasn't some old reply
							if ok && appendEntryReply.Term > currentTerm && currentTerm == rf.GetCurrentTerm() {
								LogInfo("Demoting the Leader %s to Follower", rf.id)
								rf.mu.Lock()
								rf.state = FOLLOWER
								rf.votedFor = NULL
								rf.currentTerm = appendEntryReply.Term
								rf.persist()
								rf.mu.Unlock()
							}
							if !ok {
								LogWarning("HeartBeat not sent to serverIndex %d from server = %s", j, rf.id)

							}
						}
					}
				}(i)
			}
		}
	}

	sendLogMessage()
	LogInfo("[----------Message broadcasting Completed----------] by %s ", rf.id)
}


/*
If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N,
and log[N].term == currentTerm:set commitIndex = N
*/
func (rf *Raft) attemptCommit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	LogInfo("[attemptCommit] matchIndex=%v rf.log=%v rf.commitIndex=%d", rf.matchIndex, rf.log, rf.commitIndex)
	for i := len(rf.log) - 1; i > rf.commitIndex && rf.state == LEADER; i-- {
		LogInfo("[attemptCommit] forLoop")
		if rf.currentTerm == rf.log[i].Term {
			// check if majority of matchIndex[] has value >= i
			count := 1
			for j := 0; j < rf.totalServers; j++ {
				if j != rf.me && rf.matchIndex[j] >= i {
					count++
					LogInfo("[attemptCommit] Server %s count = %d", rf.id, count)
				}
			}
			if count > rf.totalServers/2 {
				//majority found
				rf.commitIndex = i
				return
			}
		}

	}
	LogInfo("[attemptCommit] Server %s completed", rf.id)
}


func (rf *Raft) applyCommit() {

	for rf.isAlive.Get() {
		rf.mu.Lock()
		commitIndex := rf.commitIndex
		lastApplied := rf.lastApplied
		rfstring := rf.stringify()
		length := len(rf.log)
		state := rf.state
		rf.mu.Unlock()
		if state == LEADER {
			rf.attemptCommit()
		}
		LogInfo("[==COMMIT==] %s Committing message with log length %d. lastApplied = %d and commitIndex = %d",
			rfstring, length, lastApplied, commitIndex)
		flag := false
		rf.mu.Lock()
		for i := lastApplied + 1; i <= commitIndex; i++ {
			flag = true
			rf.applyCh <- ApplyMsg{CommandIndex: i, Command: rf.log[i].Command, CommandValid: true}
		}
		if flag {
			rf.lastApplied = max(commitIndex, lastApplied)
		}
		rf.mu.Unlock()
		LogInfo("[==MESSAGE COMMITTED==] Server %s lastApplied = %d and commitIndex = %d", rf.id, lastApplied, commitIndex)
		<-time.After(100*time.Millisecond)
	}
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	LogInfo("Server killed %s", rf.id)
	rf.isAlive.Set(false)
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
	rf.applyCh = applyCh
	rf.log = []LogEntry{{0, "dummy"}}
	rf.nextIndex = make([]int, rf.totalServers)
	rf.matchIndex = make([]int, rf.totalServers)
	rf.isAlive = new(AtomicBool)
	rf.isAlive.Set(true)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.initElection()
	go rf.applyCommit()


	return rf
}

const (
	ElectionMinTime = 150
	ElectionMaxTime = 300
)

func (rf *Raft) initElection() {

	for rf.isAlive.Get() {
		for rf.GetCurrentState() == LEADER {
			time.Sleep(1 * time.Second)
		}
		//time.Sleep(150 * time.Millisecond)
		time.Sleep(time.Millisecond * time.Duration(ElectionMinTime+rand.Int63n(ElectionMaxTime-ElectionMinTime)))
		if time.Now().Sub(rf.GetLastHeartBeat()) >= (500+time.Duration(rand.Intn(100)))*time.Millisecond {
			//start en election. reset the timeout
			LogInfo("[==TIMEOUT==] Election Timed out by %v. New Election by %s", time.Now().Sub(rf.GetLastHeartBeat()), rf.ToString())
			rf.startElectionAsCandidate()
		}

	}

}

func (rf *Raft) startElectionAsCandidate() {

	//increment current Term
	rf.mu.Lock()
	rf.currentTerm++
	//Transition to candidate
	rf.state = CANDIDATE
	//Vote for itself
	rf.votedFor = rf.id
	//reseting election timeout
	rf.lastHeartBeat = time.Now()
	lastLogIndex, lastLogTerm := rf.getLastEntryInfo()
	requestVoteArgs := &RequestVoteArgs{rf.currentTerm, rf.id, lastLogIndex, lastLogTerm}
	rf.persist()
	rf.mu.Unlock()

	voteChannel := make(chan bool)
	//invoke RequestVote rpcs in parallel to each server to get vote
	for i := 0; i < rf.totalServers; i++ {
		if i != rf.me {
			//Construct requestArg and invoke the api
			go func(j int) {
				//Get vote by calling the rpc
				var requestVoteReply RequestVoteReply
				ok := rf.sendRequestVote(j, requestVoteArgs, &requestVoteReply)
				rf.mu.Lock()
				//rf.currentTerm = max(rf.currentTerm, requestVoteReply.Term)
				//rf.currentTerm == requestVoteArgs.Term to check if it is not outdated term
				if rf.currentTerm < requestVoteReply.Term && rf.currentTerm == requestVoteArgs.Term {
					LogInfo("%s Stepping down from election since replyTerm = %d and candidateTerm = %d", rf.id, requestVoteReply.Term, rf.currentTerm)
					rf.state = FOLLOWER
					rf.currentTerm = requestVoteReply.Term
					rf.votedFor = NULL
					rf.persist()
				}
				rf.mu.Unlock()
				if ok {
					voteChannel <- requestVoteReply.VoteGranted && rf.GetCurrentTerm() == requestVoteArgs.Term
				} else {
					LogWarning("Request Vote Failed. Requester id = %s and requesting to = %d for Term = %d", rf.id, j, rf.GetCurrentTerm())
					voteChannel <- false
				}
			}(i)
		}
	}
	count := 1 //1 is for self vote

	select {
	case x := <-rf.countVotes(voteChannel):
		count = x
	case <-time.After(time.Duration(ElectionMinTime+rand.Int63n(ElectionMaxTime-ElectionMinTime)) * time.Millisecond):
		LogWarning("[==TIMEOUT==] [ID : %s Total Votes = %d Term = %d] Election couldn't select leader withing 0.5 seconds", rf.id, count, rf.GetCurrentTerm())
		rf.SetLastHeartBeat(time.Now())
	}

	LogInfo("ID=%s And Vote Collected = %d ", rf.id, count)
	if count > rf.totalServers/2 && rf.GetCurrentState() == CANDIDATE {
		LogInfo("Elected Leader id %v from term = %d", rf.id, rf.GetCurrentTerm())
		rf.promoteToLeader()
	}

	//server din't receive enough votes
	if count <= rf.totalServers/2 && rf.GetCurrentState() != FOLLOWER {
		LogInfo("Server with id %s din't receive enough votes (Count = %d) for Term %d", rf.id, count, rf.GetCurrentTerm())
		rf.promoteToFollower()
	}

	rf.mu.Lock()
	rf.persist()
	rf.mu.Unlock()

}

func (rf *Raft) promoteToLeader() {
	//initialize leader
	rf.mu.Lock()
	for i := 0; i < rf.totalServers; i++ {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
	LogInfo("Leader is initialized")
	rf.state = LEADER
	rf.mu.Unlock()
	go rf.handleClientMessages()
}

func (rf *Raft) promoteToFollower() {
	rf.SetCurrentState(FOLLOWER)
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


func (rf *Raft) ToString() string {
	return fmt.Sprintf("[ID: %s, State: %s, Term: %d]", rf.id, rf.GetCurrentState(), rf.GetCurrentTerm())
}

func (rf *Raft) stringify() string {
	return fmt.Sprintf("[ID: %s, State: %s, Term: %d]", rf.id, rf.state, rf.currentTerm)
}

