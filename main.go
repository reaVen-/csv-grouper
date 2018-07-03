package main

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

/* Golang concurrent CSV grouper

1. sort the metadata file first by userID then Timestamp (last write wins)
2. find x good offsets. make sure two chunks does not work on the same userID
3. spin up x go routines that reads from offset[x] until offset[x+1]
	3a. gather all (name, value) pairs into User.Map
	3b. create json of user.Map
4. let all go routines rendesvouz and calculate offsets for writing
5. spin up x go routines that writes data into grouped file
*/

type User struct {
	Map    map[string]string
	UserID string
	JSON   string
	Size   int
}

type Chunk struct {
	Number int
	Size   int
	JSON   string
}

func main() {
	raw := "users_metadata.csv"
	sorted := "go.csv"
	out := "grouped.csv"

	delim := []byte{'\x01'}
	newline := []byte{'\x0A'}

	sortStart := time.Now()
	if ok := sortFile(raw, sorted); !ok {
		return
	}
	sortElapsed := time.Since(sortStart)
	log.Infof("cut and sorted %s into %s in %s", raw, sorted, sortElapsed)

	groupStart := time.Now()
	GroupData(sorted, out, 4, delim, newline)
	groupElapsed := time.Since(groupStart)
	log.Infof("grouped %s into %s in %s", sorted, out, groupElapsed)

}

func GroupData(in, out string, concurrency int, delim, sep []byte) {
	inFile, err := os.Open("go.csv")
	if err != nil {
		log.Errorf("failed to open file: %v", err)
	}
	outFile, err := os.Create("grouped.csv")
	if err != nil {
		log.Errorf("failed to create file: %v")
	}

	streamFile(inFile, outFile, concurrency, delim, sep)
}

func newUser(userID string) *User {
	return &User{UserID: userID, Map: make(map[string]string)}
}

// Streams a file Concurrently
func streamFile(in *os.File, out *os.File, concurreny int, delim, sep []byte) {
	fileInfo, err := in.Stat()
	if err != nil {
		log.Errorf("failed to stat file")
		return
	}

	size := fileInfo.Size() //length in bytes

	log.Infof("Started parsing file with %d bytes", size)
	offsets := findOffsets(in, int(size), concurreny)
	log.Infof("using offsets: %v", offsets)

	log.Infof("Spinning up %d worksers (%d offsets)", concurreny, len(offsets))
	workChan := make(chan *Chunk)
	for x := 0; x < concurreny; x++ {
		go parseChunk(in, offsets[x], offsets[x+1], x, workChan, delim, sep)

	}

	//Gather all the chunks so we can figure out their offsets for writing
	chunks := make([]Chunk, concurreny)
	for x := 0; x < concurreny; x++ {
		chunk := <-workChan
		chunks[x] = *chunk
	}

	//sort them by chunk.Number
	sort.Slice(chunks, func(a, b int) bool {
		if chunks[a].Number < chunks[b].Number {
			return true
		}
		return false
	})

	//fan out to writeChunk()
	log.Infof("Starting %d writers", len(chunks))
	offset := 0
	wg := &sync.WaitGroup{}
	for i, chunk := range chunks {
		wg.Add(1)
		log.Infof("started writer: %d", i)
		go writeChunk(out, chunk.JSON, offset, wg)
		offset += chunk.Size
	}
	wg.Wait()
	log.Infof("Finished grouping metadata from %s into %s", in.Name(), out.Name())
}

func writeChunk(out *os.File, data string, offset int, wg *sync.WaitGroup) {
	written, err := out.WriteAt([]byte(data), int64(offset))
	if err != nil || written != len(data) {
		panic("bad write")
	}
	log.Infof("Wrote chunk")
	wg.Done()
}

func parseChunk(file *os.File, current, next, chunkNumber int, workChan chan *Chunk, delim, sep []byte) {
	log.Infof("worker %d starting...", chunkNumber)
	buf := make([]byte, next-current)
	read, err := file.ReadAt(buf, int64(current))
	if err != nil || read != next-current {
		panic("sum tin wong")
	}
	users := []*User{}
	parser(buf, &users, chunkNumber, delim, sep)
	totalSize := 0
	totalJSON := strings.Builder{}
	for _, user := range users {
		totalSize += user.Size
		totalJSON.WriteString(user.JSON)
	}
	chunk := &Chunk{
		Size:   totalSize,
		JSON:   totalJSON.String(),
		Number: chunkNumber,
	}
	log.Infof("worker %d finished parsing my chunk [%d -> %d] size: %d", chunkNumber, current, next, chunk.Size)
	workChan <- chunk
}

func parser(buf []byte, users *[]*User, workerNumber int, delim, sep []byte) {
	records := bytes.Split(buf, sep)
	current := newUser("None")
	recordSize := len(records)
	log.Infof("Worker %d: Started parsing %d records...", workerNumber, recordSize)
	for _, record := range records[0 : recordSize-1] {
		fields := bytes.Split(record, delim)
		if len(fields) != 4 {
			log.Infof("broken record: %s", fields)
			panic("broken record")
		}

		id := string(fields[0])
		if current.UserID == "None" {
			current.UserID = id
			*users = append(*users, current)
		} else if current.UserID == id {
			//log.Infof("updating same user")
		} else {
			current = newUser(id)
			*users = append(*users, current)
		}

		current.Map[string(fields[1])] = string(fields[2])
	}

	userSize := len(*users)
	log.Infof("Worker %d: started to create json of %d users...", workerNumber, userSize)
	for _, user := range *users {
		s := new(strings.Builder)
		s.WriteString(user.UserID + string(delim) + `{"metadata":{`)

		itemCount := len(user.Map)
		count := 0
		for name, value := range user.Map {
			count++
			if itemCount > count {
				s.WriteString("\"" + name + "\":\"" + value + "\",")
			} else {
				s.WriteString("\"" + name + "\":\"" + value + "\"")
			}
		}
		s.WriteString("}\n")
		user.JSON = s.String()
		user.Size = len(user.JSON)
	}

}

// betweenUsers makes sure we dont give two identical userids to the same parser
func betweenUsers(buf []byte) (int, bool) {
	// get the start of the first record in buf
	offset := bytes.IndexByte(buf, byte('\x0A'))
	if offset == -1 {
		return 0, false
	}

	userID := "None"
	records := bytes.Split(buf[offset:], []byte{'\x0A'})
	delta := offset
	for _, record := range records {
		fields := bytes.Split(record, []byte{'\x01'})
		//get first user in chunk
		if userID == "None" {
			userID = string(fields[0])
		}
		//check if userid field has changed since last record
		if userID != string(fields[0]) {
			return delta, true
		}
		delta += len(record) + 1
	}
	return 0, false
}

//scan for x good offsets (\n)
func findOffsets(file *os.File, size int, parallelizm int) []int {
	offsets := make([]int, parallelizm+1)
	offsets[0] = 0              //start reading from 0
	offsets[parallelizm] = size //end reading at size

	//do some math to set initial test offsets
	remainder := size % parallelizm
	offsets[1] = (size / parallelizm) + remainder
	for x := 2; x < parallelizm; x++ {
		offsets[x] = offsets[1] * x
	}

	//find the closest /n from offset
	for x := 1; x < parallelizm; x++ {
		lookAhead := 10240 //works for now (can implement retry functionality if it isn't found)
		buf := make([]byte, lookAhead)
		n, err := file.ReadAt(buf, int64(offsets[x]))
		if err != nil || n != lookAhead {
			panic("sum tin wong")
		}

		//make sure cutoff point is between two different userIds as well
		delta, ok := betweenUsers(buf)
		if !ok {
			panic("unable to find a user")
		}
		offsets[x] = offsets[x] + delta
	}
	return offsets

}

func sortFile(in, out string) bool {
	sortCommand := fmt.Sprintf("export LC_ALL=C && tail -n +2 %s | cut -d $'\\001' -f 2,3,4,7 | sort -n -t $'\\001' -k1,1 -k4,4 > %s", in, out)
	log.Infof("cutting and sorting %s into %s", in, out)
	_, err := exec.Command("bash", "-c", sortCommand).Output()
	if err != nil {
		fmt.Errorf("failed to execute command: %s error: %v", sortCommand, err)
		return false
	}
	log.Infof("finished cutting and sorting %s into %s", in, out)
	return true
}

func concurrentSort(in, out string) bool {
	//TODO
	return false
}
