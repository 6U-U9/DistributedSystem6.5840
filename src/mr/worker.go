package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"sync"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey struct {
	list      []KeyValue
	bucketnum int
}

// for sorting by key.
func (a ByKey) Len() int      { return len(a.list) }
func (a ByKey) Swap(i, j int) { a.list[i], a.list[j] = a.list[j], a.list[i] }
func (a ByKey) Less(i, j int) bool {
	return (ihash(a.list[i].Key) % a.bucketnum) < (ihash(a.list[j].Key) % a.bucketnum)
}

// Init, Working, Finished
var INIT = 0
var WORKING = 1
var FINISHED = 2

type worker struct {
	State   int
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
	mutex   sync.Mutex
}

func (w *worker) HeartBeat() error {
	args := HeartBeatArgs{}
	args.State = w.GetState()
	args.WorkerUid = os.Getpid()
	reply := HeartBeatReply{}
	//log.Println(args)
	ok := call("Coordinator.HeartBeat", &args, &reply)
	if ok {
		//fmt.Println("call success", os.Getpid(), reply)
		switch reply.Operation {
		case 0:
			// no operation
		case 1:
			// map
			w.SetState(WORKING)
			go w.Map(reply.InputFiles, reply.OutputPrefix, reply.OutputNumber, reply.Index)
		case 2:
			// reduce
			w.SetState(WORKING)
			go w.Reduce(reply.InputFiles, reply.OutputPrefix, reply.OutputNumber)
		case 3:
			// exit
			os.Exit(1)
		default:
			fmt.Printf("unknown operation %d\n", reply.Operation)
		}
	} else {
		fmt.Printf("call failed!\n")
	}
	return nil
}

func (w *worker) GetState() int {
	w.mutex.Lock()
	defer w.mutex.Unlock()
	return w.State
}

func (w *worker) SetState(state int) {
	w.mutex.Lock()
	w.State = state
	w.mutex.Unlock()
}

func (w *worker) Map(InputFiles []string, OutputPrefix string, OutputNumber int, Index int) error {
	defer w.SetState(FINISHED)
	if len(InputFiles) != 1 {
		fmt.Printf("Wrong input file number for map: %d", len(InputFiles))
	}

	intermediate := []KeyValue{}
	for _, filename := range InputFiles {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := w.mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}

	sort.Sort(ByKey{
		list:      intermediate,
		bucketnum: OutputNumber,
	})

	// write file
	j := 0
	for i := 0; i < OutputNumber; i++ {
		ofile, _ := os.Create(fmt.Sprintf("%s-%d-%d", OutputPrefix, Index, i))
		enc := json.NewEncoder(ofile)
		for j < len(intermediate) && i == ihash(intermediate[j].Key)%OutputNumber {
			err := enc.Encode(&intermediate[j])
			if err != nil {
				log.Fatalf("cannot write json %v", intermediate[j])
			}
			j++
		}
		ofile.Close()
	}

	return nil
}

func (w *worker) Reduce(InputFiles []string, OutputPrefix string, OutputNumber int) error {
	defer w.SetState(FINISHED)

	input := make(map[string][]string)
	for _, filename := range InputFiles {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			input[kv.Key] = append(input[kv.Key], kv.Value)
		}
		file.Close()
	}

	output := make(map[string]string)
	for k, v := range input {
		output[k] = w.reducef(k, v)
	}

	// write file
	ofile, _ := os.Create(fmt.Sprintf("%s-%d", OutputPrefix, OutputNumber))
	for k, v := range output {
		fmt.Fprintf(ofile, "%v %v\n", k, v)
	}
	ofile.Close()
	return nil
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	var w = worker{
		State:   INIT,
		mapf:    mapf,
		reducef: reducef,
		mutex:   sync.Mutex{},
	}
	go func() {
		for {
			w.HeartBeat()
			time.Sleep(500 * time.Millisecond)
		}
	}()
	wait := sync.WaitGroup{}
	wait.Add(1)
	wait.Wait()
	// map phase: (receive filename)
	// open file -> run map function -> save to files based on key
	//

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
