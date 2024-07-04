package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	nReduce := fetchNReduce()
	nMap := fetchNMap()
	// map
	for {
		mapID, filename := fetchMapTask()
		if mapID == -2 {
			// map complete
			break
		} else if mapID == -1 {
			// map assign finish
			continue
		}
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := io.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()
		kva := mapf(filename, string(content))
		// save to file and report to coordinater
		mname := make(map[int]*os.File)

		for _, kv := range kva {
			reduceID := ihash(kv.Key) % nReduce
			mnamep := fmt.Sprintf("mr-%d-%d", mapID, reduceID)
			mfile := func() *os.File {
				if file, ok := mname[reduceID]; ok {
					return file
				} else {
					mfile, err := os.CreateTemp(".", mnamep)
					if err != err {
						log.Fatalf("create %s-tmp error", mnamep)
					}
					mname[reduceID] = mfile
					return mfile
				}
			}()
			enc := json.NewEncoder(mfile)
			enc.Encode(&kv)
		}
		for reduceID, file := range mname {
			name := file.Name()
			file.Close()
			os.Rename(name, fmt.Sprintf("mr-%d-%d", mapID, reduceID))
			os.Remove(name)
		}
		completeMap(mapID)
	}

	/// start reduce after finish
	for {
		reduceID := fetchReduceTask()
		if reduceID == -1 {
			continue
		} else if reduceID == -2 {
			break
		}
		ofilename := fmt.Sprintf("mr-out-%d", reduceID)
		ofile, err := os.CreateTemp(".", ofilename)
		if err != nil {
			log.Fatalf("crate %s-tmp error", ofilename)
		}

		var kva []KeyValue
		for mapID := 0; mapID < nMap; mapID += 1 {
			mfilename := fmt.Sprintf("mr-%d-%d", mapID, reduceID)
			mfile, err := os.OpenFile(mfilename, os.O_RDONLY, 0644)
			if err != nil {
				continue
			}
			dec := json.NewDecoder(mfile)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kva = append(kva, kv)
			}
		}

		sort.Sort(ByKey(kva))

		i := 0
		for i < len(kva) {
			j := i + 1
			for j < len(kva) && kva[j].Key == kva[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, kva[k].Value)
			}
			output := reducef(kva[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

			i = j
		}

		otmpname := ofile.Name()
		ofile.Close()
		err = os.Rename(otmpname, ofilename)
		if err != nil {
			fmt.Println(err)
		}
		os.Remove(otmpname)

		completeReduce(reduceID)
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// get task
func fetchMapTask() (id int, filename string) {
	args := RPCArgs{}
	reply := RPCReply{}
	ok := call("Coordinator.MapTask", &args, &reply)
	if ok {
		id = reply.Y1
		filename = reply.Y2
		// if id > 0 {
		// 	fmt.Printf("Map:%d fetch with filename%s\n", id, filename)
		// }
		return
	} else {
		fmt.Printf("call maptask fail\n")
	}
	return
}

func fetchReduceTask() (id int) {
	args := RPCArgs{}
	reply := RPCReply{}
	ok := call("Coordinator.ReduceTask", &args, &reply)
	if ok {
		id = reply.Y1
		// if id > 0 {
		// 	fmt.Printf("Reduce:%d fetch\n", id)
		// }
		return
	} else {
		fmt.Printf("call reduce task fail\n")
	}
	return
}

// get nReducer
func fetchNReduce() int {
	args := RPCArgs{}
	reply := RPCReply{}
	ok := call("Coordinator.NReduce", &args, &reply)
	if ok {
		// fmt.Printf("nReduce fetch\n")
		return reply.Y1
	} else {
		fmt.Printf("call fetchnreduce fail\n")
	}
	return -1
}

// get nMapTask
func fetchNMap() int {
	args := RPCArgs{}
	reply := RPCReply{}
	ok := call("Coordinator.NMap", &args, &reply)
	if ok {
		// fmt.Printf("nMap fetch\n")
		return reply.Y1
	} else {
		fmt.Printf("call fetchnreduce fail\n")
	}
	return -1
}
func completeMap(id int) {
	args := RPCArgs{}
	reply := RPCReply{}
	args.X1 = id
	ok := call("Coordinator.CompleteMap", &args, &reply)
	// fmt.Printf("Map:%d Complete\n", id)
	if !ok {
		fmt.Printf("call map complete error")
	}
}

func completeReduce(id int) {
	args := RPCArgs{}
	reply := RPCReply{}
	args.X1 = id
	ok := call("Coordinator.CompleteReduce", &args, &reply)
	// fmt.Printf("Reduce:%d Complete\n", id)
	if !ok {
		fmt.Printf("call reduce complete error")
	}
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
