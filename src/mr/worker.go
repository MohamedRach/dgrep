package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string) string) {

	for true {
		reply := CallCoordinator() //RPC call to coordinator
		if reply.TaskType == "map" {
			executeMap(mapf, reply)
		} else if reply.TaskType == "reduce" {
			executeReduce(reducef, reply)
		} else {
			break
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// The worker's map task code will need a way to store intermediate key/value pairs in files in a way that can be correctly read back during reduce tasks.

func executeMap(mapf func(string, string) []KeyValue, reply MrReply) {
	chunk := reply.Chunk

	kva := mapf(chunk, reply.Target)
	kvap := ArrangeIntermediate(kva, reply.NReduce)
	files := []string{}
	for i := range kvap {
		values := kvap[i]
    filename := "mr-int- " + strconv.Itoa(i)
		ofile, _ := os.Create(filename)

		// Use Go's encoding/json package to write key/value pairs in JSON format to an open file:

		enc := json.NewEncoder(ofile)
		for _, kv := range values {
			err := enc.Encode(&kv)
			if err != nil {
				log.Fatal("error: ", err)
			}
		}
		files = append(files, filename)
		NotifyCoordinator(i, filename)
		ofile.Close()
	}
	NotifyMapSuccess(chunk)
}

func executeReduce(reducef func(string) string, reply MrReply) {
	intermediate := []KeyValue{}
	for _, v := range reply.Files {
		file, err := os.Open(v)
		if err != nil {
			log.Fatalf("cannot open %v", v)
		}

		// Use Go's encoding/json package to read the file with key/value pairs written in JSON format:

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(intermediate))

	oname := "mr-out-" + strconv.Itoa(reply.Index)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(intermediate) {
		output := reducef(intermediate[i].Key)
		fmt.Fprintf(ofile, "%v\n",output)
		i++
	}
	NotifyReduceSuccess(reply.Index)
}

func NotifyMapSuccess(chunk string) {
	args := NotifyMapSuccessArgs{}
	args.Chunk = chunk
	reply := NotifyReply{}
	call("Coordinator.NotifyMapSuccess", &args, &reply)
}

func NotifyReduceSuccess(reduceIndex int) {
	args := NotifyReduceSuccessArgs{}
	args.ReduceIndex = reduceIndex
	reply := NotifyReply{}
	call("Coordinator.NotifyReduceSuccess", &args, &reply)
}

//

// for index 0 of map task for file0, if nReduce =3

// files mr-0–0, mr-0–1, mr-0–2 will be created. Then for e.g.,

// apple will be put into mr-0–0, banana mr-0–1 etc.

// Also, index 1 of map task does the same process for file1,

// apple will be put  into mr-1–0, banana mr-1–1 etc.

// All apples will be put into mr-*-0, bananas mr-*-1. So counting will work perfectly.

// All intermediate files will be notified with NotifyCoordinator function.

func ArrangeIntermediate(kva []KeyValue, nReduce int) [][]KeyValue {
	kvap := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		v := ihash(kv.Key) % nReduce
		kvap[v] = append(kvap[v], kv)
	}
	return kvap
}



func CallCoordinator() MrReply {
	args := MrArgs{}
	reply := MrReply{}
	call("Coordinator.DistributeTask", &args, &reply)
	return reply
}

func NotifyCoordinator(reduceIndex int, file string) {
	args := NotifyIntermediateArgs{}
	args.ReduceIndex = reduceIndex
	args.File = file
	reply := NotifyReply{}
	call("Coordinator.NotifyIntermediateFile", &args, &reply)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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

