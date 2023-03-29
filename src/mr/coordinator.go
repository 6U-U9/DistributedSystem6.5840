package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

var PHASE_INIT = 0
var PHASE_MAP = 1
var PHASE_REDUCE = 2
var PHASE_EXIT = 3
var STATE_INIT = 0
var STATE_WORKING = 1
var STATE_FINISHED = 2
var WORKER_NONE = 0
var WORKER_IDLE = 1
var WORKER_WORKING = 2
var WORK_READY = 0
var WORK_ASSIGNED = 1
var WORK_FINISHED = 2
var OP_IDLE = 0

type Work struct {
	Index        int
	State        int      // 0, not assigned; 1, working; 2, finished
	InputFiles   []string // if Reduce, shuffle by key
	OutputPrefix string   // filename prefix
	OutputNumber int      // if map, nReduce; if reduce, bucketnum
}

type Coordinator struct {
	// Your definitions here.
	intermediatePrefix string
	outputPrefix       string
	workers            map[int]int // 0, none/fault; 1, idle; 2, working
	heartbeatTime      map[int]time.Time
	inputfiles         []string
	phase              int
	reduceNumber       int
	workNumbers        int
	finishedNumbers    int
	works              []Work
	workAssigns        map[int]int
	//workerHolds        map[int]int
	mutex sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) initPhase() error {
	c.phase++
	//log.Printf("Init phase %d", c.phase)
	if c.phase == PHASE_MAP {
		c.workNumbers = len(c.inputfiles)
		c.finishedNumbers = 0
		c.works = make([]Work, c.workNumbers)
		for i, v := range c.inputfiles {
			c.works[i] = Work{
				Index:        i,
				State:        WORK_READY,
				InputFiles:   []string{v},
				OutputPrefix: c.intermediatePrefix,
				OutputNumber: c.reduceNumber,
			}
		}
		c.workAssigns = map[int]int{}
		c.heartbeatTime = make(map[int]time.Time)
		c.workers = map[int]int{}
	}
	if c.phase == PHASE_REDUCE {
		temp := make([]Work, c.reduceNumber)
		for i, _ := range temp {
			temp[i] = Work{
				Index:        i,
				State:        WORK_READY,
				InputFiles:   make([]string, c.workNumbers),
				OutputPrefix: c.outputPrefix,
				OutputNumber: i,
			}
			for j := 0; j < c.workNumbers; j++ {
				temp[i].InputFiles[j] = c.intermediatePrefix + fmt.Sprintf("-%d-%d", j, i)
			}
		}

		c.workNumbers = c.reduceNumber
		c.finishedNumbers = 0
		c.works = temp
		c.workAssigns = map[int]int{}
		//c.workerHolds = map[int]int{}
	}
	if c.phase >= PHASE_EXIT {
		c.workNumbers = 1
		c.finishedNumbers = 0
	}
	return nil
}

func (c *Coordinator) HeartBeat(args *HeartBeatArgs, reply *HeartBeatReply) error {

	//fmt.Println(args)
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// update heartbeat
	c.heartbeatTime[args.WorkerUid] = time.Now()
	reply.WorkerUid = os.Getpid()

	// deal with state
	switch args.State {
	case STATE_INIT:
		c.workers[args.WorkerUid] = 1
	case STATE_WORKING:
		reply.Operation = OP_IDLE
		return nil
	case STATE_FINISHED:
		work, exists := c.workAssigns[args.WorkerUid]
		if exists {
			c.works[work].State = WORK_FINISHED
			c.finishedNumbers++
			delete(c.workAssigns, args.WorkerUid)
		}
		c.workers[args.WorkerUid] = WORKER_IDLE
	default:
		return http.ErrBodyNotAllowed
	}

	// choose a work for worker
	for c.finishedNumbers == c.workNumbers {
		c.initPhase()
	}
	var work Work
	var index int = -1
	for i, v := range c.works {
		switch v.State {
		case WORK_READY:
			work = v
			index = i
			break
		default:
			continue
		}
	}
	if index != -1 {
		c.workers[args.WorkerUid] = WORKER_WORKING
		c.workAssigns[args.WorkerUid] = work.Index
		c.works[index].State = WORK_ASSIGNED
		reply.Index = work.Index
		reply.Operation = c.phase
		reply.InputFiles = work.InputFiles
		reply.OutputNumber = work.OutputNumber
		reply.OutputPrefix = work.OutputPrefix
	} else {
		reply.Operation = OP_IDLE
	}
	return nil
}

func (c *Coordinator) check() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	t := time.Now()
	for i, v := range c.heartbeatTime {
		//log.Printf("At %v: %d last beat %v", t, i, v)
		if c.workers[i] == WORKER_WORKING && t.Sub(v) > time.Second {
			log.Printf("At %v: %d failed %v", t.Sub(v), i, v)
			c.workers[i] = WORKER_NONE
			c.works[c.workAssigns[i]].State = WORK_READY
			delete(c.workAssigns, i)
		}
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	c.mutex.Lock()
	ret := c.phase >= PHASE_EXIT
	c.mutex.Unlock()
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		intermediatePrefix: "mr-inter",
		outputPrefix:       "mr-out",
		inputfiles:         files,
		reduceNumber:       nReduce,
	}
	// Your code here.
	c.initPhase()
	go func() {
		for {
			c.check()
			time.Sleep(500 * time.Millisecond)
		}
	}()
	c.server()
	return &c
}
