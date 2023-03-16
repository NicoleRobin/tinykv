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
	"fmt"
	"github.com/pingcap-incubator/tinykv/log"
	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//	snapshot/first.....applied....committed....stabled.....last
//	--------|------------------------------------------------|
//	                          log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	// Your Code Here (2A).
	log := &RaftLog{
		storage: storage,
	}
	firstIndex, _ := storage.FirstIndex()
	log.committed = firstIndex - 1
	log.applied = firstIndex - 1
	return log
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// allEntries return all the entries not compacted.
// note, exclude any dummy entries from the return value.
// note, this is one of the test stub functions you need to implement.
func (l *RaftLog) allEntries() []pb.Entry {
	// Your Code Here (2A).
	result := []pb.Entry{}
	// filter dummy entry
	for _, entry := range l.entries {
		result = append(result, entry)
	}
	return result
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	// Your Code Here (2A).
	result := []pb.Entry{}
	for _, entry := range l.entries {
		if entry.GetIndex() > l.stabled {
			result = append(result, entry)
		}
	}
	return result
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	// Your Code Here (2A).
	result := []pb.Entry{}
	for _, entry := range l.entries {
		if entry.GetIndex() > l.applied && entry.GetIndex() <= l.committed {
			result = append(result, entry)
		}
	}
	return result
}

// LastIndex return the last index of the log entries
// 这里有问题呀，如果是返回最后一个索引，那当entries为空时应该返回-1才对，但是uint64又限制了不能返回负数
func (l *RaftLog) LastIndex() uint64 {
	// Your Code Here (2A).
	if len(l.entries) > 0 {
		return l.entries[len(l.entries)-1].Index
	}
	t, err := l.storage.LastIndex()
	if err == nil {
		return t
	}
	return 0
}

func (l *RaftLog) LastTerm() uint64 {
	lastIndex := l.LastIndex()
	lastTerm, err := l.Term(lastIndex)
	if err == nil {
		return lastTerm
	}
	return 0
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	for _, entry := range l.entries {
		if entry.Index == i {
			return entry.Term, nil
		}
	}

	// 尝试从storage中获取对应的entry记录并返回其term值
	t, err := l.storage.Term(i)
	if err == nil {
		return t, nil
	}
	return 0, fmt.Errorf("cannot find specify index")
}

func (l *RaftLog) matchTerm(i, term uint64) bool {
	t, err := l.Term(i)
	if err != nil {
		return false
	}
	return t == term
}

func (l *RaftLog) append(entries ...pb.Entry) uint64 {
	l.entries = append(l.entries, entries...)
	return l.LastIndex()
}

func (l *RaftLog) commitTo(tocommit uint64) {
	if l.committed < tocommit {
		if l.LastIndex() < tocommit {
			log.Panicf("lastIndex:%d is little than tocommit:%d", l.LastIndex(), tocommit)
		}
		l.committed = tocommit
	}
}

// 相同term并且大于当前commit才可提交
func (l *RaftLog) maybeCommit(maxIndex, term uint64) bool {
	if maxIndex > l.committed && l.zeroTermOnErrCompacted(l.Term(maxIndex)) == term {
		l.commitTo(maxIndex)
		return true
	}
	return false
}

func (l *RaftLog) zeroTermOnErrCompacted(t uint64, err error) uint64 {
	if err == nil {
		return t
	}
	if err == ErrCompacted {
		return 0
	}
	return 0
}

func (l *RaftLog) appliedTo(index uint64) {
	l.applied = index
}
