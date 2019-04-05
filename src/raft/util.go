package raft

import (
	"log"
	"sync/atomic"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func LogInfo(format string, a ...interface{}) {
	if Debug > 0 {
		log.Printf("[INFO] " + format + " \n", a...)
	}
}


func LogWarning(format string, a ...interface{}) {
	if Debug > 0 {
		log.Printf("[WARNING] " + format + " \n", a...)
	}
}


func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

type AtomicBool struct {flag int32}

func (b *AtomicBool) Set(value bool) {
	var i int32 = 0
	if value {i = 1}
	atomic.StoreInt32(&(b.flag), int32(i))
}

func (b *AtomicBool) Get() bool {
	if atomic.LoadInt32(&(b.flag)) != 0 {return true}
	return false
}
