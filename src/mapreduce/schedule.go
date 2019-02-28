package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//

type WorkerManager struct {
	c chan string
}

func (manager *WorkerManager) fetchIdleWorker() string {
	return <-manager.c
}

func (manager *WorkerManager) freeWorker(worker string) {
	go func() {
		manager.c <- worker
	}()
}

func (manager *WorkerManager) initialize(registerChan chan string) {
	manager.c = registerChan
}


func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//


	// Code
	// Continuously read from registerChan for register worker
	// Assign worker map or reduce task on some other thread
	// Keep track of map and reduce work that is to be done and or yet to be completed
	// For each map task and reduce task, it stores the state (idle, in-progress, or completed), and the identity of the worker machine (for non-idle tasks)
	// For each completed map task, the master stores the locations and sizes of the R intermediate file regions produced by the map task.
	//if map task is completed : mark it completed and add it to the reduce work list otherwise mark it idle if failed

	wkm := new(WorkerManager)
	wkm.initialize(registerChan)

	var wg sync.WaitGroup
	wg.Add(ntasks)
	for i := 0 ; i < ntasks ; i++ {

		//fetch idle worker
		workerAddress := wkm.fetchIdleWorker()

		go func(x int) {
			mapTaskArg := DoTaskArgs{jobName, mapFiles[x], phase, x, n_other}
			call(workerAddress, "Worker.DoTask", mapTaskArg, nil)
			//return this worker to the idle pool
			wkm.freeWorker(workerAddress)
			wg.Done()
		}(i)
	}
	wg.Wait()


	fmt.Printf("Schedule: %v done\n", phase)
}
