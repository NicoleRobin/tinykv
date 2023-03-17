// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"errors"
	"fmt"
	"github.com/pingcap-incubator/tinykv/log"
	"sort"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// None is a placeholder node ID used when there is no leader.
const None uint64 = 0

// StateType represents the role of a node in a cluster.
type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

var stmap = [...]string{
	"StateFollower",
	"StateCandidate",
	"StateLeader",
}

func (st StateType) String() string {
	return stmap[uint64(st)]
}

// ErrProposalDropped is returned when the proposal is ignored by some cases,
// so that the proposer can be notified and fail fast.
var ErrProposalDropped = errors.New("raft proposal dropped")

// Config contains the parameters to start a raft.
type Config struct {
	// ID is the identity of the local raft. ID cannot be 0.
	ID uint64

	// peers contains the IDs of all nodes (including self) in the raft cluster. It
	// should only be set when starting a new raft cluster. Restarting raft from
	// previous configuration will panic if peers is set. peer is private and only
	// used for testing right now.
	peers []uint64

	// ElectionTick is the number of Node.Tick invocations that must pass between
	// elections. That is, if a follower does not receive any message from the
	// leader of current term before ElectionTick has elapsed, it will become
	// candidate and start an election. ElectionTick must be greater than
	// HeartbeatTick. We suggest ElectionTick = 10 * HeartbeatTick to avoid
	// unnecessary leader switching.
	ElectionTick int
	// HeartbeatTick is the number of Node.Tick invocations that must pass between
	// heartbeats. That is, a leader sends heartbeat messages to maintain its
	// leadership every HeartbeatTick ticks.
	HeartbeatTick int

	// Storage is the storage for raft. raft generates entries and states to be
	// stored in storage. raft reads the persisted entries and states out of
	// Storage when it needs. raft reads out the previous state and configuration
	// out of storage when restarting.
	Storage Storage
	// Applied is the last applied index. It should only be set when restarting
	// raft. raft will not return entries to the application smaller or equal to
	// Applied. If Applied is unset when restarting, raft might return previous
	// applied entries. This is a very application dependent configuration.
	Applied uint64
}

func (c *Config) validate() error {
	if c.ID == None {
		return errors.New("cannot use none as id")
	}

	if c.HeartbeatTick <= 0 {
		return errors.New("heartbeat tick must be greater than 0")
	}

	if c.ElectionTick <= c.HeartbeatTick {
		return errors.New("election tick must be greater than heartbeat tick")
	}

	if c.Storage == nil {
		return errors.New("storage cannot be nil")
	}

	return nil
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64
}

type Raft struct {
	id uint64

	Term uint64 // Term是任期，每完成一次选举就加1
	// #TODO: 该vote什么时候重置呢？
	Vote uint64 // 记录此次选举投票给了哪个node

	// the log
	RaftLog *RaftLog

	// log replication progress of each peers
	Prs map[uint64]*Progress

	// this peer's role
	State StateType

	// votes records
	votes map[uint64]bool

	// msgs need to send
	msgs []pb.Message

	// the leader id
	Lead uint64

	// heartbeat interval, should send
	heartbeatTimeout int
	// baseline of election interval
	electionTimeout int
	currentET       int
	// number of ticks since it reached last heartbeatTimeout.
	// only leader keeps heartbeatElapsed.
	heartbeatElapsed int
	// Ticks since it reached last electionTimeout when it is leader or candidate.
	// Number of ticks since it reached last electionTimeout or received a
	// valid message from current leader when it is a follower.
	electionElapsed int

	// leadTransferee is id of the leader transfer target when its value is not zero.
	// Follow the procedure defined in section 3.10 of Raft phd thesis.
	// (https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
	// (Used in 3A leader transfer)
	leadTransferee uint64

	// Only one conf change may be pending (in the log, but not yet
	// applied) at a time. This is enforced via PendingConfIndex, which
	// is set to a value >= the log index of the latest pending
	// configuration change (if any). Config changes are only allowed to
	// be proposed if the leader's applied index is greater than this
	// value.
	// (Used in 3A conf change)
	PendingConfIndex uint64
}

// newRaft return a raft peer with the given config
func newRaft(c *Config) *Raft {
	if err := c.validate(); err != nil {
		panic(err.Error())
	}

	// Your Code Here (2A).
	raftLog := newLog(c.Storage)

	r := &Raft{
		id:               c.ID,
		Lead:             None,
		RaftLog:          raftLog,
		Prs:              make(map[uint64]*Progress),
		electionTimeout:  c.ElectionTick,
		heartbeatTimeout: c.HeartbeatTick,
	}

	for _, peerId := range c.peers {
		r.Prs[peerId] = &Progress{
			Next: 1,
		}
	}

	if c.Applied > 0 {
		raftLog.appliedTo(c.Applied)
	}
	r.becomeFollower(r.Term, None)
	return r
}

// sendAppend sends an append RPC with new entries (if any) and the
// current commit index to the given peer. Returns true if a message was sent.
func (r *Raft) sendAppend(to uint64) bool {
	// Your Code Here (2A).
	ents := r.RaftLog.unstableEntries()
	log.Debugf("ents:%+v", ents)
	if len(ents) > 0 {
		mEnts := []*pb.Entry{}
		for _, ent := range ents {
			mEnts = append(mEnts, &ent)
		}

		msg := pb.Message{
			MsgType:  pb.MessageType_MsgAppend,
			To:       to,
			From:     r.id,
			Term:     r.Term,
			LogTerm:  r.Term,
			Index:    r.RaftLog.LastIndex() - (uint64)(len(ents)),
			Entries:  mEnts,
			Commit:   r.RaftLog.committed,
			Snapshot: nil,
			Reject:   false,
		}
		r.msgs = append(r.msgs, msg)
		return true
	} else {
		return false
	}
}

// sendHeartbeat sends a heartbeat RPC to the given peer.
func (r *Raft) sendHeartbeat(to uint64) {
	// Your Code Here (2A).
	commit := min(r.Prs[to].Match, r.RaftLog.committed)
	msg := pb.Message{
		To:      to,
		MsgType: pb.MessageType_MsgHeartbeat,
		From:    r.id,
		Commit:  commit,
	}
	r.send(msg)
}

// sendVote sends a vote req RPC to the given peer
func (r *Raft) sendVote(to uint64) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVote,
		To:      to,
		From:    r.id,
		Term:    r.Term,
	}
	r.msgs = append(r.msgs, msg)
}

// sendVodeResp sends a vote resp to the given peer
func (r *Raft) sendVoteResp(to uint64, reject bool) {
	msg := pb.Message{
		MsgType: pb.MessageType_MsgRequestVoteResponse,
		To:      to,
		From:    r.id,
		Term:    r.Term,
		Reject:  reject,
	}
	r.msgs = append(r.msgs, msg)
}

// tick advances the internal logical clock by a single tick.
func (r *Raft) tick() {
	// Your Code Here (2A).
	// 在该函数中需要做的事情：
	// 1、leader在heartbeatTimeout时间到的时候发送heartbeat心跳
	r.heartbeatElapsed++
	if r.State == StateLeader {
		if r.heartbeatElapsed >= r.heartbeatTimeout {
			r.heartbeatElapsed = 0
			err := r.Step(pb.Message{From: r.id, To: r.id, MsgType: pb.MessageType_MsgBeat})
			if err != nil {
				log.Errorf("r.Step() failed, err:%s", err)
			}
		}
	}

	r.electionElapsed++
	if r.electionElapsed >= r.currentET {
		r.electionElapsed = 0
		if r.State == StateFollower || r.State == StateCandidate {
			err := r.Step(pb.Message{From: r.id, To: r.id, MsgType: pb.MessageType_MsgHup})
			if err != nil {
				log.Errorf("r.Step() failed, err:%s", err)
			}
		}
	}
}

// becomeFollower transform this peer's state to Follower
func (r *Raft) becomeFollower(term uint64, lead uint64) {
	// Your Code Here (2A).
	r.reset(term)
	r.Lead = lead
	r.State = StateFollower
}

// becomeCandidate transform this peer's state to candidate
func (r *Raft) becomeCandidate() {
	// Your Code Here (2A).
	r.reset(r.Term + 1)
	r.Vote = r.id
	r.State = StateCandidate
}

// becomeLeader transform this peer's state to leader
func (r *Raft) becomeLeader() {
	// Your Code Here (2A).
	// NOTE: Leader should propose a noop entry on its term
	r.reset(r.Term)
	r.Lead = r.id
	r.State = StateLeader

	r.appendEntries(pb.Entry{Data: nil})
}

// Step the entrance of handle message, see `MessageType`
// on `eraftpb.proto` for what msgs should be handled
func (r *Raft) Step(m pb.Message) error {
	// Your Code Here (2A).
	switch {
	case m.Term == 0:
	case m.Term > r.Term:
	case m.Term < r.Term:
	}
	switch m.MsgType {
	// local message
	case pb.MessageType_MsgHup:
		var term uint64
		if r.State != StateLeader {
			entries := r.RaftLog.nextEnts()
			if len(entries) > 0 && r.RaftLog.committed > r.RaftLog.applied {
				return nil
			}
			r.becomeCandidate()
			term = r.Term

			if r.quorum() == r.poll(r.id, pb.MessageType_MsgRequestVoteResponse, true) {
				r.becomeLeader()
				return nil
			}

			for id := range r.Prs {
				if id == r.id {
					continue
				}

				r.send(pb.Message{
					Term:    term,
					To:      id,
					MsgType: pb.MessageType_MsgRequestVote,
					Index:   r.RaftLog.LastIndex(),
					LogTerm: r.RaftLog.LastTerm(),
				})
			}
		} else {
			log.Panicf("%x ignoring MsgHup because already leader", r.id)
		}
	case pb.MessageType_MsgBeat:
		if r.State == StateLeader {
			for peerId, _ := range r.Prs {
				if peerId == r.id {
					continue
				}
				r.sendHeartbeat(peerId)
			}
		}
	case pb.MessageType_MsgAppend:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleAppendEntries(m)
	case pb.MessageType_MsgRequestVote:
		// #TODO: 怎么实现一个term之内只能投递一次呢？
		// term在什么时候更新？vote在什么时候重置？
		if m.Term == r.Term {
			if r.Vote == None {
				r.becomeFollower(m.Term, m.From)
				r.Vote = m.From
				r.sendVoteResp(m.From, false)
			} else if r.Vote == m.From {
				r.sendVoteResp(m.From, false)
			} else {
				r.sendVoteResp(m.From, true)
			}
		} else if m.Term > r.Term {
			r.becomeFollower(m.Term, m.From)
			r.Vote = m.From
			r.sendVoteResp(m.From, false)
		}
	case pb.MessageType_MsgRequestVoteResponse:
		if r.State == StateCandidate {
			r.votes[m.From] = !m.Reject
		}

		if r.isMajority() {
			// 获得大多数投票，成为leader
			r.becomeLeader()
		}
	case pb.MessageType_MsgPropose:
		// propose
		for i, entry := range m.Entries {
			if entry.EntryType == pb.EntryType_EntryConfChange {
				if r.PendingConfIndex > 0 {
					m.Entries[i] = &pb.Entry{
						EntryType: pb.EntryType_EntryNormal,
					}
				}
			}
		}

		entries := []pb.Entry{}
		for _, entry := range m.Entries {
			entries = append(entries, *entry)
		}
		r.appendEntries(entries...)
		r.bcastAppend()
		return nil
	case pb.MessageType_MsgAppendResponse:
		// 更新进度
		r.Prs[m.From].Match = m.Index
		mis := make(uint64Slice, 0, len(r.Prs))
		for id := range r.Prs {
			mis = append(mis, r.Prs[id].Match)
		}

		sort.Sort(sort.Reverse(mis))
		mci := mis[r.quorum()-1]
		r.RaftLog.committed = mci
	default:
		log.Errorf("unknown msg type:%+v", m.MsgType)
	}
	return nil
}

func (r *Raft) stepLeader(m pb.Message) error {
	pr, prOK := r.Prs[m.From]
	if !prOK {
		return nil
	}

	switch m.MsgType {
	case pb.MessageType_MsgBeat:
		r.bcastHeartbeat()
		return nil
	case pb.MessageType_MsgPropose:
		for _, entry := range m.Entries {
			if entry.EntryType == pb.EntryType_EntryConfChange {
			}
		}
		entries := []pb.Entry{}
		for _, entry := range m.Entries {
			entries = append(entries, *entry)
		}
		r.appendEntries(entries...)
		r.bcastAppend()
	case pb.MessageType_MsgHeartbeatResponse:
		if pr.Match < r.RaftLog.LastIndex() {
			r.sendAppend(m.From)
		}
	}
	return nil
}

func (r *Raft) stepCandidate(m pb.Message) error {
	return nil
}

func (r *Raft) stepFollower(m pb.Message) error {
	switch m.MsgType {
	case pb.MessageType_MsgPropose:
		if r.Lead == None {
			return nil
		}
		m.To = r.Lead
		r.send(m)
	case pb.MessageType_MsgHeartbeat:
		r.electionElapsed = 0
		r.Lead = m.From
		r.handleHeartbeat(m)
	default:
		return fmt.Errorf("unexpected MsgType:%d", m.MsgType)
	}
	return nil
}

func (r *Raft) isMajority() bool {
	voteCount := 0
	for _, vote := range r.votes {
		if vote {
			voteCount++
		}
	}
	if voteCount > len(r.Prs)/2 {
		// 获得大多数投票，成为leader
		return true
	}
	return false
}

// handleAppendEntries handle AppendEntries RPC request
func (r *Raft) handleAppendEntries(m pb.Message) {
	// Your Code Here (2A).
	if m.Index < r.RaftLog.committed {
		r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: r.RaftLog.committed})
		return
	}

	if mlastIndex, ok := r.RaftLog.maybeAppend(m.Index, m.LogTerm, m.Commit, m.Entries); ok {
		r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgAppendResponse, Index: mlastIndex})
	} else {
		hintIndex := min(m.Index, r.RaftLog.LastIndex())
		hintIndex = r.RaftLog.findConflictByTerm(hintIndex, m.LogTerm)
		hintTerm, err := r.RaftLog.Term(hintIndex)
		if err != nil {
			panic(fmt.Sprintf("term(%d) must be valid, but got %v", hintIndex, err))
		}
		r.send(pb.Message{
			To:      m.From,
			MsgType: pb.MessageType_MsgAppendResponse,
			Index:   m.Index,
			Reject:  true,
			LogTerm: hintTerm,
		})
	}
}

// handleHeartbeat handle Heartbeat RPC request
func (r *Raft) handleHeartbeat(m pb.Message) {
	// Your Code Here (2A).
	r.RaftLog.committed = m.Commit
	r.send(pb.Message{To: m.From, MsgType: pb.MessageType_MsgHeartbeatResponse})
}

// handleSnapshot handle Snapshot RPC request
func (r *Raft) handleSnapshot(m pb.Message) {
	// Your Code Here (2C).
}

// addNode add a new node to raft group
func (r *Raft) addNode(id uint64) {
	// Your Code Here (3A).
}

// removeNode remove a node from raft group
func (r *Raft) removeNode(id uint64) {
	// Your Code Here (3A).
}

func (r *Raft) reset(term uint64) {
	if r.Term != term {
		r.Term = term
		r.Vote = None
	}
	r.Lead = None

	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.votes = make(map[uint64]bool)
	for id := range r.Prs {
		r.Prs[id] = &Progress{
			Next: r.RaftLog.LastIndex() + 1,
		}
		if id == r.id {
			r.Prs[id].Match = r.RaftLog.LastIndex()
		}
	}
}

func (r *Raft) send(m pb.Message) {
	m.From = r.id
	if m.MsgType == pb.MessageType_MsgRequestVote {
		if m.Term == 0 {
			log.Panicf("term should be set when sending %s", m.MsgType)
		}
	} else {
		if m.Term != 0 {
			log.Panicf("...")
		}

		if m.MsgType != pb.MessageType_MsgPropose {
			m.Term = r.Term
		}
	}
	r.msgs = append(r.msgs, m)
}

func (r *Raft) appendEntries(entries ...pb.Entry) {
	// 重新修正所有将要附加的entry的Index的值
	li := r.RaftLog.LastIndex()
	for i := range entries {
		entries[i].Term = r.Term
		entries[i].Index = li + 1 + uint64(i)
	}
	r.RaftLog.append(entries...)

	// 修改当前节点的进度值
	lastIndex := r.RaftLog.LastIndex()
	if r.Prs[r.id].Match < lastIndex {
		r.Prs[r.id].Match = lastIndex
	}
	if r.Prs[r.id].Next < lastIndex+1 {
		r.Prs[r.id].Next = lastIndex + 1
	}
	r.maybeCommit()
}

func (r *Raft) bcastAppend() {
	for peerId, _ := range r.Prs {
		if peerId == r.id {
			continue
		}
		r.sendAppend(peerId)
	}
}

func (r *Raft) bcastHeartbeat() {
	for peerId, _ := range r.Prs {
		if peerId == r.id {
			continue
		}
		r.sendHeartbeat(peerId)
	}
}

// 好聪明的办法呀，对所有的match排序，然后取中间的值即为达到半数的值
func (r *Raft) maybeCommit() bool {
	mis := make(uint64Slice, 0, len(r.Prs))
	for id := range r.Prs {
		mis = append(mis, r.Prs[id].Match)
	}

	sort.Sort(sort.Reverse(mis))
	mci := mis[r.quorum()-1]
	return r.RaftLog.maybeCommit(mci, r.Term)
}

func (r *Raft) quorum() int {
	return len(r.Prs)/2 + 1
}

func (r *Raft) poll(id uint64, msgType pb.MessageType, v bool) (granted int) {
	if _, ok := r.votes[id]; !ok {
		r.votes[id] = v
	}

	for _, vv := range r.votes {
		if vv {
			granted++
		}
	}
	return granted
}
