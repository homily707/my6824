package raft

import (
	"fmt"
	"log"
	"strconv"
	"strings"
)

// Debugging
const Debug = true
const LogKey = "log"

var icon = []string{"📕️", "🧡", "💚", "💙", "💜", "💛", "🤍", "🤎", "💓"}

func RaftPrint(index int, role Role, term int, leader int, format string) (n int, err error) {
	i := index % len(icon)
	prefix := icon[i] + strconv.Itoa(index) + fmt.Sprintf("[role: %v, term: %v, leader: %v]", role, term, leader)
	if Debug {
		log.Printf(prefix + format)
	}
	return
}

func RaftPrintfWithKey(raft Raft, key string, format string, a ...interface{}) (n int, err error) {
	if key == LogKey {
		//raft.mu.Lock()
		i := raft.me % len(icon)
		prefix := icon[i] + strconv.Itoa(raft.me) + fmt.Sprintf("[role: %v, term: %v]",
			raft.role, raft.currentTerm)
		suffix := showLog(raft.logs)
		if Debug {
			log.Printf(prefix+format+suffix, a...)
		}
		//raft.mu.Unlock()
	}
	return
}

func showLog(logs []LogEntry) string {
	builder := strings.Builder{}
	builder.WriteString("{ ")
	for _, v := range logs {
		builder.WriteString(fmt.Sprintf("%v/%v ", v.Term, v.Index))
	}
	builder.WriteString(" }")
	return builder.String()
}

func RaftPrintWithKey(raft Raft, key string, format string) (n int, err error) {
	if key == LogKey {
		i := raft.me % len(icon)
		prefix := icon[i] + strconv.Itoa(raft.me) + fmt.Sprintf("[role: %v, term: %v, leader: %v, voted: %v]",
			raft.role, raft.currentTerm, raft.leader, raft.votedFor)
		if Debug {
			log.Printf(prefix + format)
		}
	}
	return
}

func RaftPrintf(index int, role Role, term int, leader int, format string, a ...interface{}) (n int, err error) {
	if LogKey == "" {
		i := index % len(icon)
		prefix := icon[i] + strconv.Itoa(index) + fmt.Sprintf("[role: %v, term: %v, leader: %v]", role, term, leader)
		if Debug {
			log.Printf(prefix+format, a...)
		}
	}
	return
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
