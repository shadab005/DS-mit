package mapreduce

import (
	"fmt"
	"sync/atomic"
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

type TaskManager struct {
	tasks chan int
	quit  chan int
	val   int64
	N     int64
}

func (t *TaskManager) initialize(ntasks int) {
	t.tasks = make(chan int)
	t.N = int64(ntasks)
	t.quit = make(chan int)
	go func() {
		for i := 0; i < ntasks; i++ {
			t.tasks <- i
		}
	}()
}

func (t *TaskManager) getTask() int {
	return <-t.tasks
}

func (t *TaskManager) addToTaskList(task int) {
	go func() {
		t.tasks <- task
	}()
}

func (t *TaskManager) completeOne() {
	atomic.AddInt64(&t.val, 1)
	if atomic.LoadInt64(&t.val) >= t.N {
		t.quit <- 1
	}
}

func (t *TaskManager) getCount() int {
	return int(atomic.LoadInt64(&t.val))
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

	t := new(TaskManager)
	t.initialize(ntasks)

	done :=0
	for done == 0{
		//fetch idle worker
		select {
		case v := <-t.tasks:
			i := v
			workerAddress := wkm.fetchIdleWorker()
			go func(x int, wk string) {
				mapTaskArg := DoTaskArgs{jobName, mapFiles[x], phase, x, n_other}
				ok := call(wk, "Worker.DoTask", mapTaskArg, nil)
				if ok {
					wkm.freeWorker(wk)
					t.completeOne()
				} else {
					t.addToTaskList(x)
				}
			}(i, workerAddress)

		case v := <-t.quit:
			fmt.Println("Quiting", v)
			done = 1
			break
		}
	}

	fmt.Printf("Schedule: %v done\n", phase)
}
