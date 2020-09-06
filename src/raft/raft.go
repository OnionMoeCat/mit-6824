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

import "sync"
import "sync/atomic"
import "../labrpc"
import "time"
import "math/rand"

// import "bytes"
// import "../labgob"

func min(a, b int) int {
    if a < b {
        return a
    }
    return b
}

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

type Log struct {
	Command interface{}
	Term int
}

type Role string

const(
    Leader Role = "Leader"
    Follower = "Follower"
    Candidate = "Candidate"
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// persistent
	currentTerm int
	votedFor int
	logs []Log

	// volatile
	role Role
	lastReceived time.Time
	commitIndex int
	lastApplied int

	// volatile leader
	nextIndex []int
	matchIndex []int

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
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
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int
	CandidateId int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	// Your data here (2A).
	Term int
	PrevLogIndex int
	PrevLogTerm int
	Entries []Log
	LeaderCommit int
}

type AppendEntriesReply struct {
	// Your data here (2A).
	Term int
	Success bool
}

//
// example lastReceived RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.lastReceived = time.Now()
	} else if rf.votedFor == -1 || args.CandidateId == rf.votedFor {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.lastReceived = time.Now()
	} else {
		reply.VoteGranted = false
	}
}

func GenerateTimeout() time.Duration {
	return time.Duration(time.Duration(rand.Intn(500) + 500)* time.Millisecond)
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	// 1. return false if term is older, not from real leader
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	}
	// from this point the leader is real, so we update the last received time
	rf.lastReceived = time.Now()
	// update the term if the leader is newer
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.votedFor = -1
	}
	// candidate -> follower
	if rf.role == Candidate {
		rf.role = Follower
	}
	// 2. return false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	if len(rf.logs) <= args.PrevLogIndex || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}
	// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
	reply.Success = true
	args_i := 0
	rf_i := args.PrevLogIndex + 1 
	for ; rf_i < len(rf.logs) && args_i < len(args.Entries); args_i, rf_i = args_i + 1, rf_i + 1 {
		if rf.logs[rf_i].Term != args.Entries[args_i].Term {
			rf.logs = rf.logs[:rf_i]
			break
		}
	}
	// 4. Append any new entries not already in the log
	new_commit_index := args.LeaderCommit
	// this means leader's log is longer, so we have new entries to append, otherwise there is no entries to append
	if args_i < len(args.Entries) {
		rf.logs = append(rf.logs, args.Entries[args_i:]...)
		new_commit_index = min(new_commit_index, len(rf.logs) - 1)
	}
	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = new_commit_index
	}
	return
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
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := rf.role == Leader
	index := len(rf.logs)
	term := rf.currentTerm
	if !isLeader {
		return index, term, isLeader
	}
	rf.logs = append(rf.logs, Log{Term: term, Command: command})
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// this function is called within lock
func (rf *Raft) makeAppendEntriesArgs(i int) AppendEntriesArgs {
	args := AppendEntriesArgs{}
	args.Term = rf.currentTerm
	args.PrevLogIndex = rf.nextIndex[i] - 1
	args.LeaderCommit = rf.commitIndex
	log_entries := rf.logs[rf.nextIndex[i]:]
	args.Entries = make([]Log, len(log_entries)) 
	copy(args.Entries, log_entries)
	return args
}

func (rf *Raft) LeaderFunc() {
	rf.mu.Lock()
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i, _ := range rf.peers {
		rf.nextIndex[i] = len(rf.logs)
		rf.matchIndex[i] = len(rf.logs)
	}
	rf.mu.Unlock()
	heartbeatTimeout := time.Duration(125 * time.Millisecond)
	for {
		rf.mu.Lock()
		if rf.killed() || rf.role != Leader {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		for i, _ := range rf.peers {
			if i != rf.me {
				rpc_args := rf.makeAppendEntriesArgs(i)
				go func(server int, args AppendEntriesArgs ) {
					reply := AppendEntriesReply{}
					ok := rf.sendAppendEntries(server, &args, &reply)
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if !ok {
						return 
					}
					// always update the current term if reply is ahead
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.role = Follower
						return
					}
					// only update when it is still a leader on current term
					if rf.role != Leader || rf.currentTerm != args.Term {
						return
					}
					if reply.Success {
						rf.nextIndex[server] = args.PrevLogIndex + len(args.Entries) + 1
						rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)	
						for n := rf.matchIndex[server]; n > rf.commitIndex && rf.logs[n].Term == rf.currentTerm ; n -- {
							count := 1
							for j := 0; j < len(rf.peers); j ++ {
								if j != rf.me && rf.matchIndex[j] >= n{
									count += 1
								}
							}
							if count > len(rf.peers) / 2 {
								rf.commitIndex = n
								break
							}
						}
					} else {
						rf.nextIndex[server] --
					}
				}(i, rpc_args)
			}
		}
		time.Sleep(heartbeatTimeout)
	}
}

func (rf *Raft) LeaderElection() {
	for {
		electionTimeout := GenerateTimeout()
		startTime := time.Now()
		time.Sleep(electionTimeout)
		rf.mu.Lock()
		if rf.killed() {
			rf.mu.Unlock()
			return
		}
		if rf.role == Follower && rf.lastReceived.Before(startTime) || rf.role == Candidate {
			go rf.KickOffElection()
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) KickOffElection() {
	rf.mu.Lock()
	rf.role = Candidate
	rf.votedFor = rf.me
	rf.currentTerm += 1
	args := RequestVoteArgs{rf.currentTerm, rf.me}
	numVote := 1
	done := false
	rf.mu.Unlock()
	for i, _ := range rf.peers {
		if i != rf.me {
			go func(server int) {
				reply := RequestVoteReply{}
				ok := rf.sendRequestVote(server, &args, &reply)
				if !ok {
					return
				}
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.role != Candidate || rf.currentTerm != args.Term {
					return
				}
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.role = Follower
					rf.votedFor = -1
				}
				if reply.VoteGranted {
					numVote ++
					if done || numVote <= len(rf.peers) / 2 {
						return
					}
					done = true
					rf.role = Leader
					go rf.LeaderFunc()
				}
			}(i)
		}
	}
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
	rf.mu.Lock()
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.role = Follower
	rf.votedFor = -1
	rf.currentTerm = 0
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.logs = append(rf.logs, Log{Term: 0})
	rf.mu.Unlock()

	// Your initialization code here (2A, 2B, 2C).
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.LeaderElection()

	return rf
}
