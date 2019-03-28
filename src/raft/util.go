package raft

import (
	"log"
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
	//log.Printf("[INFO] " + format + " \n", a...)
}


func LogWarning(format string, a ...interface{}) {
	//log.Printf("[WARNING] " + format + " \n", a...)
}