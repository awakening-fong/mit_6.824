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
import "labrpc"

import (
	"runtime"
	"fmt"
	"bytes"
	"labgob"
	"time"
	"math/rand"
	"sync/atomic"
	"log"
	//"math"
	)

// import "bytes"
// import "labgob"



var timeout_net time.Duration=time.Duration(300)*time.Millisecond  //fong

var nr_nodes int32

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
	//当前是int, see start1() 中 (m.Command).(int)
	Command      interface{}
	CommandIndex int
}

type Entry struct {
	Index int
	Term int
	Val int
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

	//Persistent state on all servers
	filelock sync.Mutex
	//RequestVote
	lock_RequestVote sync.Mutex
	//AppendEntries
	lock_AppendEntries sync.Mutex
	currentTerm int
	vote_fail_for_net_dis bool //选举失败, 因为没有收到任何回应包
	vote_ing bool
	votedFor *int
	nr_entry int //inc Entry[0]
	/*
	第0项填充为{0, 0, 0}
	*/
	entries []Entry

	applyCh *chan ApplyMsg

	//Volatile state
	//index of highest log entry known to be committed
	commitIndex int //初值为1
	intmap_applied  map[int]int //XXX bitmap
	//index of highest log entry applied to state machine 
	lastApplied int
	nextIndex []int //for leader
	matchIndex []int //for leader
	isleader bool //if major
	timer_ele *time.Timer
	//lock_time_reset sync.Mutex
	lock_start sync.Mutex
	lock_ent sync.Mutex
	last_reset time.Time
	on_heartbeat bool
	//once_heartbeat sync.Once
	once_heartbeat bool
	heart_ing []bool
	timer_heartbeats *time.Timer
	ch_reset_timer_ele chan int
	//ch_exit_sent_heartbeat chan int
	is_exiting bool //trigger by Kill()

}


func time_pr(formating string, args... interface{}){
        funcName, _, line, ok := runtime.Caller(1)
        if ok {
            log.Printf("%s:%d, %s",runtime.FuncForPC(funcName).Name(), line, fmt.Sprintf(formating, args...))
        } else {
            log.Printf("%s",fmt.Sprintf(formating, args...))
		}

}

func err_pr(formating string, args... interface{}){
        funcName, _, line, ok := runtime.Caller(1)
        if ok {
            fmt.Printf("%s:%d, %s",runtime.FuncForPC(funcName).Name(),line,fmt.Sprintf(formating, args...))
        } else {
            fmt.Printf("%s",fmt.Sprintf(formating, args...))
		}

}

func info_pr(formating string, args... interface{}){
        funcName, _, line, ok := runtime.Caller(1)
        if ok {
            fmt.Printf("%s:%d, %s",runtime.FuncForPC(funcName).Name(),line,fmt.Sprintf(formating, args...))
        } else {
            fmt.Printf("%s",fmt.Sprintf(formating, args...))
		}

}

//Caller(2)
func up2_pr(formating string, args... interface{}){
        funcName, _, line, ok := runtime.Caller(2)
        if ok {
            fmt.Printf("%s:%d, %s",runtime.FuncForPC(funcName).Name(),line,fmt.Sprintf(formating, args...))
        } else {
            fmt.Printf("%s",fmt.Sprintf(formating, args...))
		}

}


func debug_pr(formating string, args... interface{}){
		//return //tmp_fong
        funcName, _, line, ok := runtime.Caller(1)
        if ok {
            fmt.Printf("%s:%d, %s",runtime.FuncForPC(funcName).Name(),line,fmt.Sprintf(formating, args...))
        } else {
            fmt.Printf("%s",fmt.Sprintf(formating, args...))
		}

}


func tmp_pr(formating string, args... interface{}){
		//return; //tmp_fong
        funcName, _, line, ok := runtime.Caller(1)
        if ok {
            fmt.Printf("%s:%d, %s",runtime.FuncForPC(funcName).Name(),line,fmt.Sprintf(formating, args...))
        } else {
            fmt.Printf("%s",fmt.Sprintf(formating, args...))
		}

}

// return currentTerm and whether this server
// believes it is the leader.
//不读取磁盘
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	var term int
	var isleader bool
	// Your code here (2A).
	//===========
	
	term=rf.currentTerm
	isleader=rf.isleader
	fmt.Printf("getstat(): me:%d term:%d isleader:%v\n", rf.me, term, isleader); //是的, 可以使用rf
	//tmp_pr("me:%d term:%d isleader:%v\n", rf.me, term, isleader); //是的, 可以使用rf
	//------------
	return term, isleader
}



//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	return //tmp_fong
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	//fong code:
	rf.filelock.Lock()
	defer rf.filelock.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	term:=rf.currentTerm
	e.Encode(term)
	//if rf.votedFor!=nil {
	e.Encode(*(rf.votedFor))
	//} else {
	//	e.Encode(nil) //right?
	//}
	rf.nr_entry=len(rf.entries)
	e.Encode(rf.nr_entry)
	//up2_pr("");
	//debug_pr("%d save term:%d votedfor:%d\n", rf.me, rf.currentTerm, *(rf.votedFor))
	for entry:=range rf.entries {
		e.Encode(entry)
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	return //tmp_fong
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
	// ---------
	//fong code
	rf.entries=make([]Entry, 0)

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	if d.Decode(&currentTerm) != nil ||
	   d.Decode(&votedFor) != nil {
		rf.currentTerm = 0
		rf.votedFor = nil
		rf.entries[0]=Entry{0,0,0}
		debug_pr("fill 0th entry\n")
	    debug_pr("decode fail\n")
	} else {
	  rf.currentTerm = currentTerm
	  rf.votedFor = &votedFor
	  debug_pr("%d decode currentTerm:%d votedFor:%d\n", rf.me, rf.currentTerm, votedFor )
	  var nr int
	  d.Decode(&nr)
	  if nr==0 {
		 rf.entries[0]=Entry{0,0,0}
		 debug_pr("fill 0th entry\n")
	  }
	  debug_pr("%d nr entry:%d\n", rf.me, nr)
	  var entry Entry
	  for i:=0;i<nr;i++ {
		d.Decode(&entry)
		rf.entries=append(rf.entries, entry)
	  }
	}  
}




//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term int //candidate’s term
	CandidateId int // candidate requesting vote
	LastLogIndex int //index of candidate’s last log entry (§5.4)
	LastLogTerm int //term of candidate’s last log entry (§5.4)
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term int //currentTerm, for candidate to update itself
	VoteGranted bool //true means candidate received vote Rece
	//To_be_follower bool
}


type AppendEntriesArgs struct {
	Term int //leader’s term
	LeaderId int //so follower can redirect clients
	PrevLogIndex int//index of log entry immediately preceding new ones
	PrevLogTerm int //term of prevLogIndex entry
	Nr_entry int //unused, to remove
	Entries []Entry //log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int//leader’s commitIndex
	LeaderLastApply int //tell follower
}

type AppendEntriesReply struct {
	Term int//currentTerm, for leader to update itself 
	Success bool//true if follower contained entry matching prevLogIndex and prevLogTerm
	IsHeart bool//xxx
	//Case int
	Is_exist bool
	Follower_LastLogIndex int
	//Follower_LastLogTerm int
}


func  (rf *Raft) reset_ele_timer(by int) {
	rf.ch_reset_timer_ele <- by
	/*
	rf.lock_time_reset.Lock()
	rf.last_reset= time.Now()
	rf.lock_time_reset.Unlock() 
	*/ 
}

/*
func  (rf *Raft) apply_msg(msg ApplyMsg) {
		tmp_pr("===%d apply %d\n",rf.me, msg.CommandIndex)
		*(rf.applyCh) <- msg 
		rf.set_lastApplied(msg.CommandIndex)

}
*/


//[start, end)
func  (rf *Raft) apply(start int, end int) {
	if start>end {
		return
	}
	if rf.isleader {
		err_pr("this func not for leader %d\n", rf.me)
		return
	}
	for i:=start;i<end;i++ {
		//tmp_pr("===%d apply %d\n",rf.me, i)
		rf.set_lastApplied(i)
	}
}

//非空 也是心跳, 对吗????
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if rf.is_exiting {
		return
	}

	//bad code
	if len(args.Entries)==0 {
		reply.IsHeart=true
	}

	if args.Term < rf.currentTerm {
		debug_pr("%d found %d notupdate:args.Term:%d < rf.currentTerm:%d\n", rf.me, args.LeaderId, args.Term, rf.currentTerm)
		reply.Term=rf.currentTerm
		reply.Success=false 
		return
	}
	/*
	FIXME 需要新添加一个纯心跳 
	因为有时  0  1  2 
	0发送2 迟迟无法到达, 
	导致1同意,  这样, 0获得过半, 
	然后0发送空, 又过一会儿, 又发送空, 这可能导致把1的冲突项给清掉, 
	但这时, 0还不是leader呢. 	 
	*/
	//
	//非空 也是心跳, 对吗????	
	rf.reset_ele_timer(args.LeaderId)

	rf.lock_AppendEntries.Lock()
	defer rf.lock_AppendEntries.Unlock()
	if rf.is_exiting {
		return
	}

	/*
	len为2, 实际有效条目为1 
	pre Idx为1, 
	isupdate 
	*/
	//Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if rf.entries[len(rf.entries)-1].Index < args.PrevLogIndex {
		reply.Term=rf.currentTerm
		reply.Success=false
		reply.Follower_LastLogIndex=rf.entries[len(rf.entries)-1].Index
		//reply.Case=2
		return 
	}

	//心跳也是 更新的时机

	//If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)

	/*
	len为2, 实际有效条目为1 
	pre为1, 
	isupdate 
	*/
	//conflicts
	if len(rf.entries)>args.PrevLogIndex {
		if rf.entries[args.PrevLogIndex].Term!=args.PrevLogTerm{ 
			//tmp:=rf.entries[0:args.PrevLogIndex]
			rf.entries=rf.entries[0:args.PrevLogIndex]
			rf.commitIndex=len(rf.entries)
			//告知leader, 请发送PrevLogIndex项过来
			if rf.commitIndex < 10 {
				debug_pr("%d after resolve confilit:%v\n", rf.me, rf.entries)
			}
			reply.Follower_LastLogIndex=args.PrevLogIndex-1
			reply.Success=false 
			return  
		}
	}
	//true if follower contained entry matching prevLogIndex and prevLogTerm
	if !(len(rf.entries)>args.PrevLogIndex) {
		reply.Success=false
		return
	}else {
		if rf.entries[args.PrevLogIndex].Term==args.PrevLogTerm {
			/*
			处理leader重复发送, 
			导致follower 出现重复项:
			[{0 0 0} {1 1 101} {2 1 102} {2 1 102}] 
			preidx为2时, len为3, 可以添加.
			故len>idx+1时 不需要添加了. 
			*/

			if len(rf.entries) > args.PrevLogIndex+1 {
				debug_pr("%d len:%d drop preidx:%d\n", rf.me, len(rf.entries), args.PrevLogIndex)
				reply.Success=false
				reply.Is_exist=true
				return 
			} else {
				reply.Success=true
			}
		} else {
			//获取到锁之后, 情况有所变化
			/*
			节点A 发送empty, 
			节点B 也发送empty, 
			节点C 从A接收条目并填充上, 然后接收到B, 而这些条目刚刚从A那里获取到了. 
			*/
			reply.Success=false
			return 
		}
	}

	debug_pr("%d before to append nr %d , len:%d\n",rf.me, args.Nr_entry, len(rf.entries))
	debug_pr("Nr_entry %d, len args:%d\n", args.Nr_entry, len(args.Entries))

	//fixme 为何Nr_entry 6, len args:5
	for i:=0;i<len(args.Entries);i++ {
		//todo lock
		debug_pr("entry: %v\n", args.Entries[i])
		rf.entries=append(rf.entries, args.Entries[i])
	}
	if len(rf.entries)<10 {
		debug_pr("%d after append len:%d, val:%v\n",rf.me, len(rf.entries), rf.entries)
	}
	rf.apply(rf.lastApplied+1,args.LeaderLastApply+1)
	//If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit>rf.commitIndex {
		if len(args.Entries)>0 {
			rf.commitIndex=min(args.LeaderCommit, args.Entries[len(args.Entries)-1].Index)
			debug_pr("===%d commitIndex now %d, lastapply%d\n",rf.me, rf.commitIndex, rf.lastApplied)
		} 

	}
	
	//todo persisit
	return

}

//
// example RequestVote RPC handler.
//

/*
Each server will vote for at most one candidate in a
given term, on a first-come-first-served basis (note: Section 5.4 adds an additional restriction on votes).
 
The restriction(指的是5.4) ensures that the leader for any given term contains all of the entries committed in previous terms  
 
*/
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.lock_RequestVote.Lock()
	defer rf.lock_RequestVote.Unlock()
	if rf.is_exiting {
		return
	}


	// Your code here (2A, 2B).
	//reply.Term= rf.currentTerm 
	//debug_pr("%d rec request vote\n", rf.me);
	//debug_pr("args term:%d, rf.currentTerm:%d\n",args.Term,rf.currentTerm);
	if args.Term < rf.currentTerm {		
		reply.Term= rf.currentTerm//仅在follower更大时, 才修改reply.Term
		reply.VoteGranted=false
		//reply.To_be_follower=true
		debug_pr("args term:%d, rf.currentTerm:%d, need to be follower\n",args.Term,rf.currentTerm);
		return 
	}

	//这里需要加锁, 否则存在会 先后同意两个leader 
	if args.Term > rf.currentTerm {
		//reply.VoteGranted=true //是的, 还不能同意, 要比较log
		debug_pr("%d update term from:%d to %d. pack from/votefor %d\n",rf.me, rf.currentTerm, args.Term, args.CandidateId);
		rf.currentTerm=args.Term
		rf.convert_follower(by_votegrand, nil)
		//debug_pr("args term:%d, rf.currentTerm:%d\n",args.Term,rf.currentTerm);
		
		//rf.votedFor=&args.CandidateId
		//rf.currentTerm=args.Term
		rf.persist()
		//return
	} 
	/*else {
		//网络隔离? 或者两个差不多同时超时
		//先来先服务, 故拒绝后来者
		debug_pr("BUG? %d rec args term:%d,== rf.currentTerm:%d\n",rf.me,args.Term,rf.currentTerm);
		reply.VoteGranted=false
		return
	}*/
	info_pr("%d found term(%d) equal from %d\n", rf.me, args.Term, args.CandidateId)
	//If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	var a bool
	var b bool
	var c bool
	var d bool

	//非 拜占庭
	//a =  *(rf.votedFor) == args.CandidateId
	a = (rf.votedFor==nil || *(rf.votedFor) == args.CandidateId)
	b =(len(rf.entries)!=0  && 
		  args.LastLogTerm > rf.entries[len(rf.entries)-1].Term)
	c = (len(rf.entries)!=0 && args.LastLogTerm == rf.entries[len(rf.entries)-1].Term && args.LastLogIndex >= rf.entries[len(rf.entries)-1].Index)
	d = len(rf.entries)==0
	if a && (b ||c ||d){
		if b {
			debug_pr("len:%d\n", len(rf.entries))
			if len(rf.entries)!=0 {
				debug_pr("rf entry Term%d\n",rf.entries[len(rf.entries)-1].Term)
			}
		}
		reply.VoteGranted=true
		//会导致问题吗? 答:不会. 这里是 投票给候选人了.
        rf.reset_ele_timer(args.CandidateId) 
		rf.convert_follower(by_votegrand, &args.CandidateId)
		debug_pr("args logterm%d logindex%d\n",args.LastLogTerm, args.LastLogIndex)
		debug_pr("%d votefor %d\n", rf.me, args.CandidateId);
		//rf.votedFor=&args.CandidateId
		if rf.currentTerm>args.Term { //其他地方更新了 currentTerm
			tmp_pr("BUG %d > args %d\n", rf.currentTerm, args.Term)
		}
		rf.persist()
		return
	}  else {
		reply.VoteGranted=false
		return
	}
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
/*
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
*/
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	timer_dis := time.NewTimer(timeout_net)
	ch_rpc:=make(chan int, 1)
	var ok bool
	go func(timer_dis *time.Timer){
		ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
		if ok {
			ch_rpc<- 1
		} else {
			ch_rpc<- 0
		}
		timer_dis.Stop()
	}(timer_dis)
	net_ok:=true
	select {
			case <-timer_dis.C:
				net_ok=false
				debug_pr("%d com to %d timeout\n", rf.me, server)
			case ret_rpc:=<-ch_rpc:
				if ret_rpc>0 {
					net_ok=true
				} else {
					net_ok=false
				}
	}
	ok=net_ok
	//不必eat channel, 对吗?
	return ok
}


/*
 
@retal false 通讯失败 
*/
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	timer_dis := time.NewTimer(timeout_net)
	ch_rpc:=make(chan int, 1)
	var ok bool
	go func(timer_dis *time.Timer){
		ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
		if ok {
			ch_rpc<-1
		} else {
			ch_rpc<-0
		}
		timer_dis.Stop()
	}(timer_dis)
	net_ok:=false
	select {
			case <-timer_dis.C:
				net_ok=false
				debug_pr("%d com to %d timeout\n", rf.me, server)
			case ret_rpc:=<-ch_rpc:
				if ret_rpc>0 {
					net_ok=true
				} else {
					net_ok=false
				}
	}
	ok=net_ok
	//不必eat channel, 对吗?
	return ok
}

//true for ok
//AppendEntries
func  (rf *Raft) send_log(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) (app_ok bool, net_dis bool) {
	if rf.is_exiting {
		debug_pr("%d is_exiting\n", rf.me)
		return false, true
	}

	for {
	if rf.is_exiting {
		debug_pr("%d is_exiting\n", rf.me)
		return false, true
	}

	if rf.sendAppendEntries(server,args,reply) {
		if reply.Success {
			if !reply.IsHeart {
				tmp_pr("%d app to %d ok,\n", rf.me, server)
			}
			/*
			nextIndex
			matchIndex
			*/
			return true, false
		} else if reply.Term > args.Term{ //比if reply.Term > rf.currentTerm 要好.
		//} else if reply.Term > rf.currentTerm{
			rf.currentTerm=reply.Term
			debug_pr("%d found self not update\n", rf.me)
			rf.convert_follower(by_rejected, nil)
			return false, false
		
		/*} else if reply.Case==2 {
			if reply.IsHeart {  //bad code
				return true, false
			} else {
				return false, false
			}*/
		} else if reply.Is_exist {
			/*
			这个算添加成功吗? 
			由于该返回值也用来判断 是否选举成功, 故这里返回成功.
			*/
			return true, false 
		} else {
			//AppendEntries
			//dec prevlog
			//XXX 
			/* 改用reply.Follower_LastLogIndex, 故这部分弃用*/

			if args.PrevLogIndex<0 {
				if reply.IsHeart {
					return true, false
				}
				//debug_pr("BUG,%d, but try to dec\n", args.PrevLogIndex)
				return true, false  //bad code
			}
			
			/*
			For example,
when rejecting an AppendEntries request, the follower can include the term of the conflicting entry and the first
index it stores for that term. With this information, the
leader can decrement nextIndex to bypass all of the conflicting entries in that term;  
			*/
			tmp_old_idx:=args.PrevLogIndex
			args.PrevLogIndex=reply.Follower_LastLogIndex
			args.PrevLogTerm=rf.entries[args.PrevLogIndex].Term
			len_entry:=len(rf.entries)
			args.Entries=rf.entries[args.PrevLogIndex+1:len_entry]

			debug_pr("%d->%d, PrevLogIndex:%d dec to %d\n", rf.me, server, tmp_old_idx, args.PrevLogIndex)
			/*
			args.PrevLogIndex--
			//args.PrevLogTerm=args.Entries[args.PrevLogIndex].Term
			args.PrevLogTerm=rf.entries[args.PrevLogIndex].Term
			args.Nr_entry++
			len_entry:=len(rf.entries)
			args.Entries=rf.entries[args.PrevLogIndex+1:len_entry] 
			*/ 
			tmp_pr("args now %v\n", args.Entries)
			continue
		}
	} else {
		debug_pr("%d com to %d fail\n", rf.me, server)
		return  false, true
	}
	}
}

func  (rf *Raft) set_lastApplied(new_follower_done int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.is_exiting {
		debug_pr("%d is_exiting\n", rf.me)
		return 
	}

	if rf.lastApplied < new_follower_done {
		if rf.isleader {
			rf.intmap_applied[new_follower_done]=1
			debug_pr("===%d mark %d apply\n", rf.me, new_follower_done)
			start:=rf.lastApplied+1
			debug_pr("===%d start %d, rf.commitIndex+1:%d \n", rf.me, start, rf.commitIndex+1)
			for  i:=start;i<rf.commitIndex+1;i++{
				_, ok := rf.intmap_applied[i] 
				if ok {
					rf.lastApplied=i;

					debug_pr("===%d lastapp %d \n", rf.me, i)
					var msg ApplyMsg
					msg.CommandValid=true
					msg.Command=rf.entries[i].Val 
					msg.CommandIndex=rf.entries[i].Index
					tmp_pr("===%d apply %d\n",rf.me, msg.CommandIndex)
					*(rf.applyCh) <- msg
				} else {
					break;
				}
			}
		} else { //not  isleader
			if rf.lastApplied+1!=new_follower_done {
				//err_pr("BUG, %d want set lastApplied %d, but !=%d(lastApplied)+1\n",rf.me, rf.lastApplied, new_follower_done )
				return
			}
			if new_follower_done>len(rf.entries)-1 {
				err_pr("BUG:%d new:%d len-1:%d\n", rf.me, new_follower_done, len(rf.entries)-1)
				return
			}
			rf.lastApplied=new_follower_done

			var msg ApplyMsg
			msg.CommandValid=true
			msg.Command=rf.entries[new_follower_done].Val 
			msg.CommandIndex=rf.entries[new_follower_done].Index
			tmp_pr("===%d apply %d\n",rf.me, msg.CommandIndex)
			*(rf.applyCh) <- msg
		}
	} else if rf.lastApplied != new_follower_done {
		//rf.lastApplied > new_follower_done
		err_pr("BUG, %d want setapply %d, but < last%d\n", rf.me, new_follower_done, rf.lastApplied)
	}
}

func  (rf *Raft) len_ent() int {
	rf.lock_ent.Lock()
	defer rf.lock_ent.Unlock()
	return len(rf.entries)
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the

//fong:没有要求转发到leader

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
	//debug_pr("%d --start %d\n", rf.me, command.(int))
	rf.lock_start.Lock()
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	isLeader=rf.isleader
	//index=rf.lastApplied+1 //第一个返回值
	//index=rf.commitIndex //本函数的第一个返回值.
	index=len(rf.entries)
	if index< rf.commitIndex {
		debug_pr("BUG? %d cmd:%d index:%d < commitidx:%d entries:%v\n", rf.me,command.(int), index, rf.commitIndex, rf.entries)
	}
	term=rf.currentTerm
	isLeader=rf.isleader
	if isLeader {
		debug_pr("%d isleader lockstart,val: %v\n", rf.me, rf.isleader);
	}
	if isLeader {
		var new Entry
		//first index is 1
		//cfg.logs[i][m.CommandIndex] = m.Command.(int)
		tmp_pr("len %d\n", len(rf.entries))
		//rf.lock_ent.Lock() //fixme need lock for entries
		new.Index=len(rf.entries)
		new.Term=rf.currentTerm
		new.Val=command.(int)
		debug_pr("new entry:index:%d term:%d val:%d\n", new.Index, new.Term, new.Val)
		//-----------
		len_old:=len(rf.entries)
		rf.entries=append(rf.entries, new)
		//rf.commitIndex++
		if  len(rf.entries)<10 {
			debug_pr("leader:%d %v\n", rf.me, rf.entries)
		}
		len_entry:=len(rf.entries)
		var args AppendEntriesArgs
		args.Term=rf.currentTerm
		args.LeaderId=rf.me
		/*
		本实现中, commitIndex为0是无用的占用项,
		因为log[]first index is 1.
		这样, 新数据是写入到 log[commitIndex].
		参数LeaderCommit表示leader 要把数据写入[LeaderCommit]
		*/
		/*
		因为follower要能够从参数中看出leader的apply变动, 故 
		方案1: 
		1. leader 往[commitIndex] 写入logA 
		2. args.LeaderCommit=commitIndex
		3. follower添加log
		4. commitIndex++ 
		5. leader根据返回值, 执行apply
		6. follower 接收的心跳中LeaderCommit 变动了, 执行apply动作
		 
		 
		方案2: 
		AppendEntriesArgs 中添加新的字段 LeaderLastApply 来实现
		*/
		args.LeaderCommit=rf.commitIndex
		args.LeaderLastApply=rf.lastApplied
		if len_old>0 {
		args.PrevLogIndex=len_old-1
		args.PrevLogTerm=rf.entries[args.PrevLogIndex].Term
		} else {
			args.PrevLogIndex=0
			args.PrevLogTerm=0
		}
		args.Nr_entry=1
		args.Entries=rf.entries[len_entry-1:len_entry]
		//debug_pr("......entry: %v\n", args.Entries)
		//tmp_nr_nodes:=len(rf.peers)
		total_nr_ci:=0
		nr_other:= int(atomic.LoadInt32(&nr_nodes))-1
		ch_ci:=make(chan int, nr_other)
		for i:=0;i< int(atomic.LoadInt32(&nr_nodes));i++ {
			if rf.me==i {//leader
				go func(){
				//fixme leader在等够major后才能commit
				var msg ApplyMsg
				msg.CommandValid=true
				msg.Command=args.Entries[0].Val 
				msg.CommandIndex=args.Entries[0].Index
				total_nr_ci++
				j:=0
				for j=0;j<nr_other;j++ {

					tmp_ci:= <- ch_ci
					total_nr_ci+=tmp_ci
					
					if total_nr_ci > (nr_other+1)/2 {
						rf.set_lastApplied(msg.CommandIndex)
						debug_pr("%d unlock done\n", rf.me);
						rf.lock_start.Unlock()
						break;
					}
				}
				if j==nr_other {
					debug_pr("%d unlock done\n", rf.me);
					rf.lock_start.Unlock()
				}
				}()
				continue  //next node
			} //leader
			tmp:=i
			tmp_args:=args
			go func(tmp int, tmp_args *AppendEntriesArgs){
				var reply AppendEntriesReply
				app,_:=rf.send_log(tmp, tmp_args, &reply)
				if app{
					ch_ci <- 1
				} else {
					ch_ci <- 0
				}

			}(tmp, &tmp_args)
		}
		rf.commitIndex++ //放到 set_lastApplied()??
	} else {
		//debug_pr("%d unlock\n", rf.me);
		rf.lock_start.Unlock()
	}
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	return
	// Your code here, if desired.
	tmp_pr("==== kill %d\n", rf.me)
	if rf.timer_ele!=nil {
		rf.timer_ele.Stop()
	}
	if rf.timer_heartbeats!=nil {
		rf.timer_heartbeats.Stop()
	}
	rf.is_exiting=true
//	rf.ch_reset_timer_ele <- rf.me
	//rf.ch_exit_sent_heartbeat
	for   i:= range rf.heart_ing {
		rf.heart_ing[i]=true
	}
	rf.vote_ing=true
	rf.convert_follower(by_votegrand, nil)
	//rf.reset_ele_timer(rf.me)
	/*
	//time.Sleep(1000 * time.Millisecond)
	//time.Sleep(1000 * time.Millisecond)
	//rf.lock_AppendEntries.Lock()
	//defer rf.lock_AppendEntries.Unlock()

	rf.currentTerm=0
	rf.commitIndex=1
	rf.lastApplied=0
	rf.votedFor=nil
	//rf.entries=nil
	rf.nr_entry=0
	rf.vote_fail_for_net_dis=false
	tmp_pr("====end of kill %d\n", rf.me)
	//time.Sleep(1000 * time.Millisecond)
	rf.entries=nil
	//rf.entries=make([]Entry)
	//rf.entries=append(rf.entries,Entry{0,0,0})

	for   i:= range rf.heart_ing {
		rf.heart_ing[i]=false
	}
	rf.vote_ing=false
	*/
	
}


/*
1. 要不要发送给自己? 答:不要. 
2. 如何序列化  网络包? 
*/
/*
网络断开  和 拒绝心跳 要分开处理 吗?
网络断开 当成 接收心跳 吗?
 
 
问题:乱序, 导致 follower先收到空entry, 后收到vote, 
follower该如何处理? 
 
*/

func  (rf *Raft) lastone_ent() *Entry {
	len1:=len(rf.entries)
	if len1 >= 1 {
		return &rf.entries[len1-1]
	} else {
		err_pr("BUG?, %d len is 0\n", rf.me)
		return nil
	}
}

//AppendEntries
//返回 false 如果发现自己不是leader
func (rf *Raft) send_empty_entry() bool{
	ch_total_accept_heart:=make(chan int, int(atomic.LoadInt32(&nr_nodes))-1)
	total_accept_heart:=0
	ch_dis_net:=make(chan int, int(atomic.LoadInt32(&nr_nodes))-1)
	total_dis_net:=0
	currentTerm_before_send:=rf.currentTerm
	lastone_idx:=rf.lastone_ent().Index

	for i:=0; i<int(atomic.LoadInt32(&nr_nodes)); i++ {
		if i==rf.me {
			total_accept_heart++
			//rf.ch_reset_timer_ele <- rf.me
			rf.reset_ele_timer(rf.me)
			continue
		}
		tmp_i:=i
		go func(tmp_i int){
			/*
			问题:前面的包因网络而迟迟不到, 并不应该阻止新的心跳包发送. 对吗? 
			 
			 
			*/
			if rf.heart_ing[tmp_i] {
				tmp_pr("%d skip heart_ing %d\n", rf.me, tmp_i)
				return
			}	
			rf.heart_ing[tmp_i]=true
			defer func() { rf.heart_ing[tmp_i]=false }()
			
			//first index is 1
			//-----------
			len_old:=len(rf.entries)
			if len_old<10 {
				debug_pr("leader:%d %v\n", rf.me, rf.entries)
			}
			//len_entry:=len(rf.entries)
			//每个节点的进度不一样, 故args的初始化放在各线程内.
			var args AppendEntriesArgs
			args.Term=rf.currentTerm
			args.LeaderId=rf.me
			if len_old>0 {
				args.PrevLogIndex=len_old-1
				args.PrevLogTerm=rf.entries[args.PrevLogIndex].Term
			} else {
				args.PrevLogIndex=0
				args.PrevLogTerm=0
			}
			args.Nr_entry=0
			//args.Entries=rf.entries[len_entry-1:len_entry]
			args.Entries=make([]Entry, 0)
			args.LeaderCommit=rf.commitIndex
			args.LeaderLastApply=rf.lastApplied
			debug_pr("%d to send empty entryto %d: preidx:%d preterm:%d term:%d\n", rf.me,tmp_i, args.PrevLogIndex, args.PrevLogTerm, args.Term)
			var reply AppendEntriesReply
			reply.Term=currentTerm_before_send
			app,dis:=rf.send_log(tmp_i, &args, &reply)
			if app{
				ch_total_accept_heart<- 1
			} else {
				ch_total_accept_heart<- 0
			}
			if dis {
				ch_dis_net<-1
			} else {
				ch_dis_net<-0
			}
		}(tmp_i)
	}

	nr_node:=int(atomic.LoadInt32(&nr_nodes))
	for i:=0;i<nr_node-1;i++{
		tmp:= <- ch_total_accept_heart
		total_accept_heart+=tmp
		// 这里可以处理   leader变动, 导致follower没有执行apply
		//进入send empty, 那么, 之前获得了vote, 故是最新的.
		//accept代表follower更新了数据, 故这里可以apply, 对吗?
		if total_accept_heart> nr_node/2 {
			//这个返回时,  lastApplied 可能早就更新过了.
			if currentTerm_before_send==rf.currentTerm {
			debug_pr("%d set apply %d in send empty\n", rf.me, lastone_idx)
			lastone_idx:=rf.lastone_ent().Index
			rf.apply(rf.lastApplied+1, lastone_idx+1)
			/*
			rf.set_lastApplied(lastone_idx) 
			*/ 
			//lastone_idx:=rf.lastone_ent().Index
			rf.commitIndex=lastone_idx
			}
		}
		//todo major 拒绝 就返回
	}

	for i:=0;i<int(atomic.LoadInt32(&nr_nodes))-1;i++{
		tmp:= <- ch_dis_net
		total_dis_net +=tmp		
	}

	//网络超时时间  >  选举超时时间
	if total_dis_net== int(atomic.LoadInt32(&nr_nodes))-1{
		rf.convert_follower(by_dis_net, nil)
		return false
	}  else if total_accept_heart<=int(atomic.LoadInt32(&nr_nodes))/2 {
		rf.convert_follower(by_rejected, nil)
		tmp_pr("====send empty, but %d not leader now\n", rf.me)
		return false
	}
	
	//这个不需要判断大数
	if currentTerm_before_send < rf.currentTerm {
		debug_pr("send empty, but %d not high\n", rf.me)
		rf.convert_follower(by_rejected, nil) //bad code 在send_log()就应该能够处理
		return false
	}
	return true
	

} 


func (rf *Raft) convert_candidate() {
	rf.isleader=false
	rf.on_heartbeat=false
	rf.send_and_get_vote()
}


const (
    by_votegrand = iota // value --> 0  //同意他人为leader
    by_rejected              // value --> 1 //参选被拒绝
    by_dis_net            // value --> 2 //网络不通
)


func (rf *Raft) convert_follower(reason int, votefor *int) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.isleader {
		debug_pr("%d convert_follower\n", rf.me)
	}
	if reason==by_votegrand {
		rf.votedFor=nil
		rf.vote_fail_for_net_dis=false //在VotedGrant时
		//todo 停止正在进行vote的
		fmt.Printf("by_votegrand\n")
	}
	if reason==by_dis_net {
		rf.votedFor=nil
		rf.vote_fail_for_net_dis=true
		fmt.Printf("by_dis_net\n")
	}  
	if reason==by_rejected {
		rf.votedFor=nil
		fmt.Printf("%d by_rejected\n", rf.me )
	}
	rf.on_heartbeat=false
	rf.isleader=false
	if votefor!=nil {
		rf.votedFor=votefor
	}
	
}


func (rf *Raft) be_leader() {
	if rf.isleader {
		time_pr("%d already leader, exit\n", rf.me)
		return
	}
	old_term:=rf.currentTerm
	if !rf.send_empty_entry(){
		return
	}
	if rf.currentTerm==old_term {
		info_pr("====%d now leader, term:%d\n", rf.me, rf.currentTerm)
		rf.commitIndex=len(rf.entries)
	}
	rf.isleader=true
	
	if rf.once_heartbeat {
		return
	}
	rf.once_heartbeat=true
	go func(rf *Raft){
		var timeout_heart time.Duration
		rf.timer_heartbeats=time.NewTimer(2 * time.Second)//some huge timeout
		rf.on_heartbeat=true
		for  {
			if rf.is_exiting {
				break;
			}
			//the tester limits you to 10 heartbeats per second 
			//所以, 频率要求 , 对吗?  要求 减低频率
			timeout_heart = time.Duration(150 + rand.Intn(50))*time.Millisecond
			time.Sleep(timeout_heart)
			//rf.timer_heartbeats.Reset(timeout_heart)
			//tmp_pr("%d heartbeat on\n", rf.me)
			//select {
			   /*
			case <-	rf.ch_exit_sent_heartbeat:
				tmp_pr("%d exit heartbeat\n", rf.me)
				rf.on_heartbeat=false
				rf.timer_heartbeats.Stop()
				break //exit for
			    return
			   */ 
			//case  <- rf.timer_heartbeats.C:
			//<- rf.timer_heartbeats.C
				if rf.on_heartbeat {
					if rf.votedFor!=nil && *(rf.votedFor)==rf.me {
						debug_pr("node %d time to heartbeats\n",rf.me)
						//rf.send_heartbeats()
						go func(){//避免一些节点网络断开, 而导致阻塞在这里.
						rf.send_empty_entry()
						}()
					}
				}
			//}//end of select
		}
		debug_pr("===%d heartbeat exit\n", rf.me);
	}(rf)


}


/*
参与者 何时保存自己的votedfor ? 
 
 
不必等所有都返回后 才成为leader
*/

//RequestVote
func (rf *Raft) send_and_get_vote() {
	//涉及currentTerm更新, 故加锁
	rf.lock_RequestVote.Lock()
	defer rf.lock_RequestVote.Unlock()

	//如果进行中, 则返回
	if rf.vote_ing {
		debug_pr("%d  vote_ing\n", rf.me)
		return
	}
	rf.vote_ing=true
	defer func() {rf.vote_ing=false }()

	total_voted:=0
	ch_total_vote := make(chan int, int(atomic.LoadInt32(&nr_nodes))-1)
	ch_net_dis:= make(chan int, int(atomic.LoadInt32(&nr_nodes))-1)
	//ch_total_vote := make(chan int)
	//tmp_pr("====len rf.peers %d\n", len(rf.peers))

	var args RequestVoteArgs
	// 避免网络隔离, 一直加的情况
	if !rf.vote_fail_for_net_dis {
	rf.currentTerm+=1 //不管选举是否成功, 都++
	}
	rf.vote_fail_for_net_dis=false
	total_voted++
	//rf.ch_reset_timer_ele <- rf.me //reset election timer
	rf.reset_ele_timer(rf.me)
	//args.Term=rf.currentTerm+1
	rf.votedFor=&rf.me
	rf.persist()
	args.Term=rf.currentTerm
	tmp_pr("canid %d, args term %d\n", rf.me, args.Term)
	args.CandidateId=rf.me
	local_be_rejected:=false
	if len(rf.entries)!=0 {
		args.LastLogTerm=rf.entries[len(rf.entries)-1].Term
		args.LastLogIndex=rf.entries[len(rf.entries)-1].Index
	} else {
		args.LastLogTerm=0
		args.LastLogIndex=0
	}
	for i:=0; i<int(atomic.LoadInt32(&nr_nodes)); i++ {
		tmp_i:=i
		if rf.me==tmp_i{
			continue
		}

		go func(tmp_i int){

			//debug_pr("%d sendRequestVote to %d\n", rf.me, tmp_i);
			var reply RequestVoteReply
			reply.Term=rf.currentTerm
			if rf.sendRequestVote(tmp_i, &args, &reply) { //通讯成功
				ch_net_dis <-0
				if reply.VoteGranted {
					debug_pr("%d VoteGranted from %d\n", rf.me, tmp_i);
					if reply.Term < rf.currentTerm { //乱序
						debug_pr("found unorder\n")
						ch_total_vote <- 0
					} else {
						ch_total_vote <- 1
					}
					//tmp_pr("ch_total_vote %d\n",tmp_i);
					//rf.persist()
					//rf.sendAppendEntries(rf.me, &args, &reply)

				} else {
					ch_total_vote<-0
					debug_pr("%d be rejected from %d\n",rf.me,tmp_i);
					if reply.Term > rf.currentTerm {
						debug_pr("%d update term from %d to %d\n",rf.me, rf.currentTerm,reply.Term);
						rf.currentTerm=reply.Term
						rf.persist()
					  //参选者 log  不够新
					}
					local_be_rejected=true
					
				}
			} else { //通讯失败
				ch_net_dis <-1
				ch_total_vote<-0
				tmp_pr("%d com to %d fail\n",rf.me, tmp_i);
			}

		}(tmp_i)
	}//end of for
	//不必等所有都返回后 才成为leader
	//debug_pr("before cal total_voted\n")
	is_majority:=false
	nr_acc_nodes:=0
	nr_other:=int(atomic.LoadInt32(&nr_nodes))-1
	total_dis:=0
	is_dis_net:=false


	for {	
		select {
			case tmp_voted := <- ch_total_vote:
				total_voted+=tmp_voted
				nr_acc_nodes++
				
			case tmp_dis := <- ch_net_dis:
				total_dis+=tmp_dis
				//nr_acc_nodes++
				
		}
		if  total_dis == nr_other {
			//处理乱序包
			if rf.currentTerm > args.Term {
				debug_pr("%d unorder \n", rf.me)
				return
				//break
			} else {
				rf.convert_follower(by_dis_net, nil)
				break
			}
		}
		if total_voted>int(atomic.LoadInt32(&nr_nodes))/2 { // 3/2=1,故这里不是>=
			is_majority=true
			//这里不能break, 要等是否有reject
			//FIXME 但是这样会导致 投票的节点 超时, 导致新的选举发生
			//break;
		} 
		if nr_acc_nodes==nr_other {
			break
		}
	}
	if is_dis_net {
		//消耗剩余chan
		left:=nr_other - nr_acc_nodes
		if left>0 {
		tmp_pr("eat left: %d\n", left)
		}
		for i:=0;i<left;i++{
			go func() {
				<- ch_total_vote
				<- ch_net_dis
			}()
		}
	}
	if local_be_rejected && is_majority {
		debug_pr("%d majority, but be reject\n", rf.me)
		is_majority=false
	}

	if is_majority {
		//消耗剩余chan
		left:=nr_other - nr_acc_nodes
		if left>0 {
		tmp_pr("%d eat left: %d\n", rf.me, left)
		}
		/*if rf.vote_fail_for_net_dis >= rf.currentTerm {
			debug_pr("%d vote(term:%d) fail for found high term\n", rf.me, args.Term)
		} else {*/
		if args.Term < rf.currentTerm {  //fixme 也可能是乱序包?
			debug_pr("%d vote(term:%d) fail, for found high term\n", rf.me, args.Term)
			rf.convert_follower(by_votegrand, nil)
		} else {
			rf.be_leader()
		}
		for i:=0;i<left;i++{
			go func() {
				<- ch_total_vote
				<- ch_net_dis
			}()
		}
	} else if total_voted<=int(atomic.LoadInt32(&nr_nodes))/2 { 
		rf.convert_follower(by_rejected, nil)
	}

	
	debug_pr("----%d total_voted >= %d\n", rf.me, total_voted)
}


//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
//fong:由于是先commit后apply, 这里apply相当于写磁盘?
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
//创建节点
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	//打印时间
	log.SetFlags(log.LstdFlags)

	rf := &Raft{}
	rf.is_exiting=false
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	rf.applyCh=&applyCh
	//Volatile
	rf.commitIndex=1 //没错, 后续log 要写入的位置 [commitIndex]
	rf.lastApplied=0
	rf.intmap_applied=make(map[int]int)
	rf.nextIndex = make([]int, 5)
	//nextIndex []int //for leader //TODO
	rf.matchIndex = make([]int, 5)
	//matchIndex []int //for leader //TODO
	
	//var tmp_nr_nodes int32
	tmp_nr_nodes:=len(rf.peers)
	rf.heart_ing=make([]bool, len(rf.peers))
	atomic.StoreInt32(&nr_nodes, int32(tmp_nr_nodes))

	rf.timer_ele = time.NewTimer(time.Duration(300 + rand.Intn(100))*time.Millisecond)
	rf.ch_reset_timer_ele=make(chan int,1)
	//rf.ch_exit_sent_heartbeat=make(chan int,1)
	rf.isleader=false
	
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	if len(rf.entries)==0 {
	rf.entries=append(rf.entries,Entry{0,0,0})
	debug_pr("fill 0th entry\n")
	}

	//Persistent
	//todo
	//currentTerm int
	//votedFor int

	
	go func() {
		//The tester requires that the leader send heartbeat RPCs no more than ten times per second. 
		//[300,400]
		//for now := range time.Tick(300 + rand.Intn(100)*time.Millisecond) {
		var timeout_ele time.Duration

		var  start1 time.Time
		for {
			if rf.is_exiting {
				break;
			}
			timeout_ele = time.Duration(550 + rand.Intn(150))*time.Millisecond
			start := time.Now()
			rf.timer_ele.Reset(timeout_ele)
			select {
			case  from:= <- rf.ch_reset_timer_ele:
				tmp_pr("%d timer reset by %d\n", rf.me, from)
				rf.timer_ele.Stop()
				start1 = time.Now()

			case <-rf.timer_ele.C:
				elapsed := time.Since(start)
				elapsed1 := time.Since(start1)
				if (elapsed < timeout_ele) || (elapsed1 <  timeout_ele) {
					debug_pr("found %d short sleep\n", rf.me)
				} else {
					if rf.isleader {
						debug_pr("no need timeout ele for leader %d\n", rf.me)
					} else {
						go func(){
							debug_pr("node %d timeout\n", rf.me)
							rf.convert_candidate()
						}()
					}
				}
			}//end of select
			
		}

    }()
	
	return rf
}
