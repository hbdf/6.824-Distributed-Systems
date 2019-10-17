package mapreduce

import (
	"fmt"
	"os"
	"encoding/json"
	"sort"
)

func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTask int, // which reduce task this is
	outFile string, // write the output here
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {

	var allData []KeyValue
	for i := 0; i < nMap; i++ {
		fileName := reduceName(jobName, i, reduceTask)

		file, err := os.OpenFile(fileName, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0777)
		if err != nil {
			fmt.Println("errouuuuu", err)
		}
		enc := json.NewDecoder(file)
		loop:
		for {
			var temp KeyValue
			err := enc.Decode(&temp)
			if err != nil {
				break loop
			}
			allData = append(allData, temp)
		}
		file.Close()
	}
	fmt.Println("Size of data from map", len(allData))

	sort.Slice(allData, func(i, j int) bool {
		return allData[i].Key < allData[j].Key
	})
	outFileReal, _ := os.OpenFile(outFile, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0777)
	enc := json.NewEncoder(outFileReal)
	for i := 0; i < len(allData); {
		first := i
		var values []string;
		for j := i; j < len(allData) ; j++ {
			if allData[j].Key == allData[first].Key {
				values = append(values, allData[j].Value)
				i = j + 1
			}
		}

		enc.Encode(KeyValue{allData[first].Key, reduceF(allData[first].Key, values)})

	}
	outFileReal.Close()
	//
	// doReduce manages one reduce task: it should read the intermediate
	// files for the task, sort the intermediate key/value pairs by key,
	// call the user-defined reduce function (reduceF) for each key, and
	// write reduceF's output to disk.
	//
	// You'll need to read one intermediate file from each map task;
	// reduceName(jobName, m, reduceTask) yields the file
	// name from map task m.
	//
	// Your doMap() encoded the key/value pairs in the intermediate
	// files, so you will need to decode them. If you used JSON, you can
	// read and decode by creating a decoder and repeatedly calling
	// .Decode(&kv) on it until it returns an error.
	//
	// You may find the first example in the golang sort package
	// documentation useful.
	//
	// reduceF() is the application's reduce function. You should
	// call it once per distinct key, with a slice of all the values
	// for that key. reduceF() returns the reduced value for that key.
	//
	// You should write the reduce output as JSON encoded KeyValue
	// objects to the file named outFile. We require you to use JSON
	// because that is what the merger than combines the output
	// from all the reduce tasks expects. There is nothing special about
	// JSON -- it is just the marshalling format we chose to use. Your
	// output code will look something like this:
	//
	// enc := json.NewEncoder(file)
	// for key := ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Your code here (Part I).
	//
}
