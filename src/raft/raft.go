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
	"6.824/labgob"
	"bytes"
	"log"
	"math/rand"
	"sort"
	"time"

	//	"bytes"
	"sync"
	"sync/atomic"

	//	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type Entry struct {
	Term    int
	Index   int
	Command interface{}
}

const (
	Leader int = iota
	Candidate
	PreCandidate
	Follower
)

const nonVote = -1
const MaxLogSize = 100

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	applyCh chan ApplyMsg

	term  int
	state int

	entries      []Entry
	startIndex   int
	commitIndex  int
	appliedIndex int

	snapshot          []byte
	lastIncludedIndex int
	lastIncludedTerm  int

	// matchIndex and nextIndex are hold by leader
	// to track replicate progress
	matchIndex []int
	nextIndex  []int

	voteFor  int
	votesAck map[int]struct{}

	electionElapsed  int
	electionTimeout  int
	heartbeatElapsed int
	heartbeatTimeout int

	requestVoteArgsCh      chan *RequestVoteArgs
	requestVoteReplyCh     chan *RequestVoteReply
	appendEntriesArgsCh    chan *AppendEntriesArgs
	appendEntriesReplyCh   chan *AppendEntriesReply
	installSnapshotArgsCh  chan *InstallSnapshotArgs
	installSnapshotReplyCh chan *InstallSnapshotReply

	getStateReqCh chan *GetStateRequest
	startReqCh    chan *StartRequest
	snapReqCh     chan *SnapRequest
}

type GetStateRequest struct {
	RespCh chan *GetStateResponse
}

type GetStateResponse struct {
	Term     int
	IsLeader bool
}

type StartRequest struct {
	Command interface{}
	RespCh  chan *StartResponse
}

type StartResponse struct {
	Term     int
	Index    int
	IsLeader bool
}

type SnapRequest struct {
	index    int
	snapshot []byte
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	req := &GetStateRequest{make(chan *GetStateResponse)}
	rf.getStateReqCh <- req
	resp := <-req.RespCh
	term, isleader = resp.Term, resp.IsLeader
	return term, isleader
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
	enc := labgob.NewEncoder(w)
	enc.Encode(rf.term)
	enc.Encode(rf.voteFor)
	enc.Encode(rf.lastIncludedIndex)
	enc.Encode(rf.lastIncludedTerm)
	enc.Encode(rf.startIndex)
	enc.Encode(rf.commitIndex)
	// enc.Encode(rf.appliedIndex)
	enc.Encode(rf.entries)
	rf.persister.SaveStateAndSnapshot(w.Bytes(), rf.snapshot)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var (
		term              int
		voteFor           int
		lastIncludedIndex int
		lastIncludedTerm  int
		startIndex        int
		commitIndex       int
		// appliedIndex      int
		entries []Entry
	)
	if d.Decode(&term) != nil || d.Decode(&voteFor) != nil ||
		d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil ||
		d.Decode(&startIndex) != nil || d.Decode(&commitIndex) != nil ||
		// d.Decode(&appliedIndex) != nil ||
		d.Decode(&entries) != nil {
		log.Fatalf("readPersist decode error")
	} else {
		rf.term = term
		rf.voteFor = voteFor
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.startIndex = startIndex
		rf.commitIndex = commitIndex
		// rf.appliedIndex = appliedIndex
		rf.entries = entries
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	req := &SnapRequest{
		index:    index,
		snapshot: snapshot,
	}
	rf.snapReqCh <- req
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	From      int
	Term      int
	LastTerm  int
	LastIndex int
	PreVote   bool

	ReplyCh chan *RequestVoteReply
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	From    int
	Term    int
	Succ    bool
	PreVote bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	args.ReplyCh = make(chan *RequestVoteReply)
	rf.requestVoteArgsCh <- args
	*reply = *(<-args.ReplyCh)
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

type AppendEntriesArgs struct {
	From int
	Term int

	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []Entry

	ReplyCh chan *AppendEntriesReply
}

type AppendEntriesReply struct {
	From int
	Term int
	Succ bool

	MatchIndex int
	HintIndex  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	args.ReplyCh = make(chan *AppendEntriesReply)
	rf.appendEntriesArgsCh <- args
	*reply = *(<-args.ReplyCh)
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

type InstallSnapshotArgs struct {
	From int
	Term int

	LastIncludedIndex int
	LastIncludedTerm  int
	Snapshot          []byte

	ReplyCh chan *InstallSnapshotReply
}

type InstallSnapshotReply struct {
	From          int
	Term          int
	Accept        bool
	IncludedIndex int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	args.ReplyCh = make(chan *InstallSnapshotReply)
	rf.installSnapshotArgsCh <- args
	*reply = *(<-args.ReplyCh)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) becomeFollower() {
	rf.state = Follower
	rf.voteFor = nonVote
	rf.electionElapsed = 0
}

func (rf *Raft) becomePreCandidate() {
	rf.state = PreCandidate

	// re-gen election timeout
	rf.electionTimeout = rand.Intn(20) + 20

	rf.votesAck = make(map[int]struct{})
	rf.votesAck[rf.me] = struct{}{}
	args := &RequestVoteArgs{
		From:      rf.me,
		Term:      rf.term + 1,
		LastTerm:  rf.termOfIndex(rf.lastIndex()),
		LastIndex: rf.lastIndex(),
		PreVote:   true,
	}
	rf.sendVoteReqs(args)
	DPrintf("[peer %d] become pre candidate in term %d, with timeout %d", rf.me, rf.term, rf.electionTimeout)
}

func (rf *Raft) becomeCandidate() {
	rf.state = Candidate
	rf.voteFor = rf.me
	rf.electionElapsed = 0
	rf.votesAck = make(map[int]struct{})
	rf.votesAck[rf.me] = struct{}{}
	args := &RequestVoteArgs{
		From:      rf.me,
		Term:      rf.term,
		LastTerm:  rf.termOfIndex(rf.lastIndex()),
		LastIndex: rf.lastIndex(),
		PreVote:   false,
	}
	rf.sendVoteReqs(args)
	DPrintf("[peer %d] become candidate in term %d", rf.me, rf.term)
}

func (rf *Raft) sendVoteReqs(args *RequestVoteArgs) {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		i := i
		go func() {
			reply := &RequestVoteReply{}
			if ok := rf.sendRequestVote(i, args, reply); ok {
				rf.requestVoteReplyCh <- reply
			}
		}()
	}
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	rf.heartbeatElapsed = 0

	// append new no-op log
	//rf.entries = append(rf.entries, Entry{
	//	Term:    rf.term,
	//	Index:   rf.lastIndex() + 1,
	//	Command: nil,
	//})

	rf.matchIndex = make([]int, len(rf.peers))
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.matchIndex[i] = 0
		rf.nextIndex[i] = rf.lastIndex() + 1
	}

	rf.sendEntries()

	DPrintf("[peer %d] become leader in term %d", rf.me, rf.term)
}

func (rf *Raft) termOfIndex(idx int) int {
	if idx == rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}
	return rf.entries[idx-rf.startIndex].Term
}

func (rf *Raft) lastIndex() int {
	return rf.startIndex + len(rf.entries) - 1
}

func (rf *Raft) copyEntries(fromIndex int, size int) []Entry {
	to := make([]Entry, 0, size)
	entries := rf.entries[fromIndex-rf.startIndex:]
	for i := 0; i < size; i++ {
		if i >= len(entries) {
			break
		}
		to = append(to, entries[i])
	}
	return to
}

// removeEntries removes size entries from head
func (rf *Raft) removeEntries(size int) {
	for i := 0; i < size; i++ {
		if len(rf.entries) == 0 {
			return
		}
		if len(rf.entries) == 1 {
			rf.entries = []Entry{}
			return
		}
		rf.entries = rf.entries[1:]
	}
}

func (rf *Raft) popEntries(fromIndex int) {
	rf.entries = rf.entries[:fromIndex-rf.startIndex]
}

func (rf *Raft) commitEntries() {
	sortMatch := make([]int, len(rf.matchIndex))
	copy(sortMatch, rf.matchIndex)
	sort.Ints(sortMatch)
	newCommit := sortMatch[len(sortMatch)/2+1]
	toCommit := max(rf.commitIndex, newCommit)
	if rf.termOfIndex(toCommit) == rf.term {
		rf.commitIndex = toCommit
	}
}

func (rf *Raft) sendEntriesTo(peer int) {
	if rf.nextIndex[peer] < rf.startIndex {
		DPrintf("[peer %d] send snapshot to %d -- %d", rf.me, peer, rf.lastIncludedIndex)
		args := &InstallSnapshotArgs{
			From:              rf.me,
			Term:              rf.term,
			LastIncludedIndex: rf.lastIncludedIndex,
			LastIncludedTerm:  rf.lastIncludedTerm,
			Snapshot:          rf.snapshot,
		}
		go func() {
			reply := &InstallSnapshotReply{}
			if ok := rf.sendInstallSnapshot(peer, args, reply); ok {
				rf.installSnapshotReplyCh <- reply
			}
		}()
		return
	}

	args := &AppendEntriesArgs{
		From:         rf.me,
		Term:         rf.term,
		LeaderCommit: rf.commitIndex,
		PrevLogIndex: rf.nextIndex[peer] - 1,
		PrevLogTerm:  rf.termOfIndex(rf.nextIndex[peer] - 1),
		Entries:      []Entry{},
	}
	if rf.lastIndex() >= rf.nextIndex[peer] {
		args.Entries = rf.copyEntries(rf.nextIndex[peer], MaxLogSize)
	}
	DPrintf("[peer %d] send entry to %d -- %v", rf.me, peer, args)
	go func() {
		reply := &AppendEntriesReply{}
		if ok := rf.sendAppendEntries(peer, args, reply); ok {
			rf.appendEntriesReplyCh <- reply
		}
	}()
}

func (rf *Raft) sendEntries() {
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		rf.sendEntriesTo(peer)
	}
}

func (rf *Raft) tick() {
	switch rf.state {
	case Follower, PreCandidate:
		rf.electionElapsed++
		if rf.electionElapsed > rf.electionTimeout {
			rf.electionElapsed = 0
			rf.becomePreCandidate()
		}
	case Candidate:
		rf.electionElapsed++
		if rf.electionElapsed > rf.electionTimeout {
			rf.electionElapsed = 0
			rf.becomeFollower()
		}
	case Leader:
		rf.heartbeatElapsed++
		if rf.heartbeatElapsed > rf.heartbeatTimeout {
			rf.heartbeatElapsed = 0
			rf.sendEntries()
		}
	}
}

func (rf *Raft) handleRequestVoteArgs(args *RequestVoteArgs) {
	reply := &RequestVoteReply{From: rf.me}
	defer func() {
		DPrintf("[peer %d] handle request vote req %v reply %v", rf.me, args, reply)
		args.ReplyCh <- reply
	}()

	if args.Term < rf.term {
		reply.Term = rf.term
		reply.Succ = false
		return
	}

	termNewer := args.LastTerm > rf.termOfIndex(rf.lastIndex())
	termSame := args.LastTerm == rf.termOfIndex(rf.lastIndex())
	logLonger := args.LastIndex >= rf.lastIndex()
	candidateLogNewer := termNewer || (termSame && logLonger)

	if args.PreVote {
		reply.Term = args.Term
		reply.PreVote = true
		if candidateLogNewer {
			reply.Succ = true
		} else {
			reply.Succ = false
		}
		return
	} else {
		reply.PreVote = false
	}

	if args.Term > rf.term {
		rf.term = args.Term
		switch rf.state {
		case Leader, PreCandidate, Candidate:
			rf.becomeFollower()
		case Follower:
			rf.voteFor = nonVote
		}
	}
	// args.Term == rf.term
	reply.Term = rf.term
	if rf.state == Follower && rf.voteFor == nonVote {
		if candidateLogNewer {
			rf.electionElapsed = 0
			rf.voteFor = args.From
			reply.Succ = true
		} else {
			reply.Succ = false
		}
	} else {
		reply.Succ = false
	}
}

func (rf *Raft) handleRequestVoteReply(reply *RequestVoteReply) {
	if reply.PreVote {
		if rf.state != PreCandidate {
			return
		}
		if reply.Term < rf.term+1 {
			return
		}
		if reply.Term > rf.term+1 {
			rf.term = reply.Term
			rf.becomeFollower()
			return
		}
		if reply.Succ {
			rf.votesAck[reply.From] = struct{}{}
			if len(rf.votesAck) > len(rf.peers)/2 {
				rf.term++
				rf.becomeCandidate()
			}
		}
	} else {
		if reply.Term < rf.term {
			return
		}
		if reply.Term > rf.term {
			rf.term = reply.Term
			rf.becomeFollower()
			return
		}
		if rf.state == Candidate && reply.Succ {
			rf.votesAck[reply.From] = struct{}{}
			if len(rf.votesAck) > len(rf.peers)/2 {
				rf.becomeLeader()
			}
		}
	}
}

func (rf *Raft) handleAppendEntriesArgs(args *AppendEntriesArgs) {
	DPrintf("[peer %d] handleAppendEntriesArgs %v", rf.me, args)
	reply := &AppendEntriesReply{From: rf.me}
	defer func() {
		args.ReplyCh <- reply
	}()

	if args.Term < rf.term {
		reply.Term = rf.term
		return
	}
	if args.Term > rf.term {
		rf.term = args.Term
		rf.becomeFollower()
	}
	reply.Term = rf.term
	switch rf.state {
	case Leader:
		log.Fatalf("[peer %d] two leaders in term %d", rf.me, rf.term)
	case Candidate, PreCandidate:
		rf.becomeFollower()
		rf.voteFor = args.From
		fallthrough
	case Follower:
		rf.electionElapsed = 0
		if args.PrevLogIndex < rf.lastIncludedIndex {
			// the log is already truncated
			reply.Succ = true
			return
		}
		if rf.lastIndex() < args.PrevLogIndex || rf.termOfIndex(args.PrevLogIndex) != args.PrevLogTerm {
			reply.Succ = false
			reply.HintIndex = rf.commitIndex + 1
			return
		}
		// prev log match
		reply.Succ = true
		reply.MatchIndex = args.PrevLogIndex + len(args.Entries)
		var newEntries []Entry
		for i := range args.Entries {
			entry := args.Entries[i]
			if entry.Index > rf.lastIndex() {
				newEntries = args.Entries[i:]
				break
			}
			if entry.Term != rf.termOfIndex(entry.Index) {
				rf.popEntries(entry.Index)
				newEntries = args.Entries[i:]
				break
			}
		}
		if newEntries != nil {
			rf.entries = append(rf.entries, newEntries...)
		}
		rf.commitIndex = max(rf.commitIndex, min(args.LeaderCommit, reply.MatchIndex))
	}
}

func (rf *Raft) handleAppendEntriesReply(reply *AppendEntriesReply) {
	DPrintf("[peer %d] handleAppendEntriesReply %v", rf.me, reply)
	if reply.Term < rf.term {
		return
	}
	if reply.Term > rf.term {
		rf.becomeFollower()
		return
	}
	if rf.state != Leader {
		return
	}
	if reply.Succ {
		rf.matchIndex[reply.From] = max(rf.matchIndex[reply.From], reply.MatchIndex)
		rf.nextIndex[reply.From] = rf.matchIndex[reply.From] + 1
		rf.commitEntries()
	} else {
		rf.nextIndex[reply.From] = reply.HintIndex
	}
}

func (rf *Raft) handleInstallSnapshotArgs(args *InstallSnapshotArgs) {
	reply := &InstallSnapshotReply{
		From: rf.me, Term: rf.term, IncludedIndex: args.LastIncludedIndex,
	}
	defer func() {
		args.ReplyCh <- reply
	}()

	if args.Term < rf.term {
		return
	}

	if args.Term > rf.term {
		rf.term = args.Term
		rf.electionElapsed = 0
		rf.becomeFollower()
		reply.Term = rf.term
	}

	switch rf.state {
	case Leader:
		log.Fatalf("[peer %d] two leaders in term %d", rf.me, rf.term)
	case PreCandidate, Candidate:
		rf.becomeFollower()
		fallthrough
	case Follower:
		rf.electionElapsed = 0
		if args.LastIncludedIndex <= rf.lastIncludedIndex {
			// already have a snapshot whose included index >= this one
			reply.Accept = true
			return
		}
		rf.lastIncludedTerm = args.LastIncludedTerm
		rf.lastIncludedIndex = args.LastIncludedIndex
		rf.snapshot = args.Snapshot
		rf.entries = []Entry{}
		rf.startIndex = args.LastIncludedIndex + 1
		rf.commitIndex = rf.lastIncludedIndex
		reply.Accept = true
	}
}

func (rf *Raft) handleInstallSnapshotReply(reply *InstallSnapshotReply) {
	if reply.Term > rf.term {
		rf.term = reply.Term
		rf.becomeFollower()
		return
	}
	if reply.Term < rf.term || rf.state != Leader || !reply.Accept {
		return
	}
	rf.matchIndex[reply.From] = max(rf.matchIndex[reply.From], reply.IncludedIndex)
	rf.nextIndex[reply.From] = rf.matchIndex[reply.From] + 1
}

func (rf *Raft) handleStartRequest(req *StartRequest) {
	if rf.state != Leader {
		req.RespCh <- &StartResponse{IsLeader: false}
		return
	}
	term := rf.term
	index := rf.startIndex + len(rf.entries)
	rf.entries = append(rf.entries, Entry{
		Term:    term,
		Index:   index,
		Command: req.Command,
	})
	req.RespCh <- &StartResponse{
		Term:     term,
		Index:    index,
		IsLeader: true,
	}
	rf.sendEntries()
}

func (rf *Raft) handleSnapRequest(req *SnapRequest) {
	DPrintf("[peer %d] snapshot index %d", rf.me, req.index)
	if req.index <= rf.lastIncludedIndex {
		return
		log.Fatalf("snap request index < log last include index")
	}
	if req.index > rf.appliedIndex {
		log.Fatalf("snap request index > log apply index")
	}
	rf.lastIncludedTerm = rf.termOfIndex(req.index)
	rf.lastIncludedIndex = req.index
	rf.snapshot = req.snapshot
	rf.removeEntries(req.index - rf.startIndex + 1)
	rf.startIndex = req.index + 1
	DPrintf("[peer %d] start index %d ents %v", rf.me, rf.startIndex, rf.entries)
}

func (rf *Raft) run() {
	tickDuration := 10 * time.Millisecond
	timer := time.NewTimer(tickDuration)

	for rf.killed() == false {
		select {
		// time module
		case <-timer.C:
			rf.tick()
			timer.Reset(tickDuration)

		// rpc request and reply handler
		case args := <-rf.requestVoteArgsCh:
			rf.handleRequestVoteArgs(args)
		case reply := <-rf.requestVoteReplyCh:
			rf.handleRequestVoteReply(reply)

		case args := <-rf.appendEntriesArgsCh:
			rf.handleAppendEntriesArgs(args)
		case reply := <-rf.appendEntriesReplyCh:
			rf.handleAppendEntriesReply(reply)

		case args := <-rf.installSnapshotArgsCh:
			rf.handleInstallSnapshotArgs(args)
		case reply := <-rf.installSnapshotReplyCh:
			rf.handleInstallSnapshotReply(reply)

		// api msg handler
		case req := <-rf.getStateReqCh:
			req.RespCh <- &GetStateResponse{
				Term:     rf.term,
				IsLeader: rf.state == Leader,
			}
		case req := <-rf.startReqCh:
			rf.handleStartRequest(req)
		case req := <-rf.snapReqCh:
			rf.handleSnapRequest(req)
		}
		// TODO: ugly apply
		DPrintf("[peer %d] last index %d apply %d include %d include term %d, start index %d", rf.me, rf.lastIndex(), rf.appliedIndex, rf.lastIncludedIndex, rf.lastIncludedTerm, rf.startIndex)
		if rf.lastIncludedIndex > rf.appliedIndex {
			rf.applyCh <- ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.snapshot,
				SnapshotTerm:  rf.lastIncludedTerm,
				SnapshotIndex: rf.lastIncludedIndex,
			}
			rf.appliedIndex = rf.lastIncludedIndex
		}
		DPrintf("[peer %d] apply %d new commit %d", rf.me, rf.appliedIndex, rf.commitIndex)
		if rf.commitIndex > rf.appliedIndex {
			for i := rf.appliedIndex + 1; i <= rf.commitIndex; i++ {
				committed := rf.entries[i-rf.startIndex]
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					Command:      committed.Command,
					CommandIndex: committed.Index,
				}
				rf.appliedIndex++
			}
		}
		rf.persist()
	}
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
	req := &StartRequest{
		Command: command,
		RespCh:  make(chan *StartResponse),
	}
	rf.startReqCh <- req
	for rf.killed() == false {
		select {
		case resp := <-req.RespCh:
			index, term, isLeader = resp.Index, resp.Term, resp.IsLeader
			DPrintf("[start at %d] index %d term %d isLeader %v", rf.me, index, term, isLeader)
			return index, term, isLeader
		default:
			// TODO: add context control?
		}
	}
	return -1, -1, false
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

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

	}
}

func (rf *Raft) initVolatile() {
	rf.state = Follower

	// election timeout range 200ms to 400ms
	// our ticker should tick every 10ms
	rf.electionElapsed = 0
	rf.electionTimeout = rand.Intn(20) + 20
	// heartbeat timeout every 150ms
	rf.heartbeatTimeout = 15

	rf.appliedIndex = 0

	rf.requestVoteArgsCh = make(chan *RequestVoteArgs, 100)
	rf.requestVoteReplyCh = make(chan *RequestVoteReply, 100)
	rf.appendEntriesArgsCh = make(chan *AppendEntriesArgs, 100)
	rf.appendEntriesReplyCh = make(chan *AppendEntriesReply, 100)
	rf.installSnapshotArgsCh = make(chan *InstallSnapshotArgs, 100)
	rf.installSnapshotReplyCh = make(chan *InstallSnapshotReply, 100)

	rf.getStateReqCh = make(chan *GetStateRequest, 100)
	rf.startReqCh = make(chan *StartRequest, 100)
	rf.snapReqCh = make(chan *SnapRequest, 100)
}

func (rf *Raft) initPersist() {
	rf.term = 0
	rf.voteFor = nonVote
	rf.startIndex = 0
	rf.commitIndex = 0
	// rf.appliedIndex = 0
	rf.lastIncludedIndex = -1
	rf.lastIncludedTerm = -1
	rf.entries = []Entry{{Term: 0, Index: 0, Command: nil}}
	rf.snapshot = []byte{}
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
	if data := persister.ReadRaftState(); len(data) > 0 {
		rf.readPersist(persister.ReadRaftState())
		rf.snapshot = persister.ReadSnapshot()
	} else {
		rf.initPersist()
	}
	rf.initVolatile()

	DPrintf("[peer %d] election timeout %d", rf.me, rf.electionTimeout)

	rf.applyCh = applyCh

	go rf.run()

	// initialize from state persisted before a crash
	// rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	// go rf.ticker()

	return rf
}

// utils
func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}
func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}
