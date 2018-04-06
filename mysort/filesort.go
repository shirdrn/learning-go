package mysort

import (
	"fmt"
	"sync"
	"os"
	"strconv"
	"path/filepath"
	"io/ioutil"
	"strings"
)

/////////////////////////
// FILE SORT
////////////////////////

const (
	BlockElementSize = 4
)

type fileMeta struct {
	id int
	dir string
	fileName string
}

func NewFileMeta(id int, dir string, fileName string) *fileMeta {
	return &fileMeta{
		id: id,
		dir: dir,
		fileName: fileName,
	}
}

func (fm *fileMeta) Check() {
	_, err := os.Stat(fm.dir)
	if err != nil {
		panic("Dir " + fm.dir + " doesn't exist!")
	}
}

type ReqValue struct {
	ReaderId int
	SeekerId int
	Value int
}

/**
  Assume data storage structure:
  partition file suffix with '.data', block index files suffix with '.index', such as
    part-00000.data  part-00000.index
    part-00001.data  part-00001.index
  We can locate each block data.
 */
type FileSort struct {
	RootDir string
	OutputPath string
	outFile *os.File
	recordReaders map[int]*FileRecordReader
}

func (fs *FileSort) isSeekerCompleted(readerId, seekerId int) bool {
	return fs.recordReaders[readerId].offsetSeekers[seekerId].IsCompleted
}

func (fs *FileSort) isSeekersCompleted() bool {
	for _, reader := range fs.recordReaders {
		for _, seeker := range reader.offsetSeekers {
			if !seeker.IsCompleted {
				return false
			}
		}
	}
	return true
}

func (fs *FileSort) Sort(partitionFiles []string) {
	fs.recordReaders = make(map[int]*FileRecordReader)

	// create file record readers
	totalBlocks := 0
	for i, file := range partitionFiles {
		meta := NewFileMeta(i, fs.RootDir, file)
		reader := NewFileRecordReader(i, meta)
		reader.Initialize()
		fs.recordReaders[i] = reader
		totalBlocks += reader.blockCount
		fmt.Println("Reader created: readerId=", i, ", blockCount=", reader.blockCount)
	}
	fmt.Println("Reader info: readerCount=", len(fs.recordReaders), ", totalBlocks=", totalBlocks)
	ch := make(chan ReqValue, totalBlocks)
	defer close(ch)

	// open output file for writing sorted results
	fs.outFile, _ = os.Create(fs.OutputPath)
	defer fs.outFile.Close()

	// start file record readers
	wg := &sync.WaitGroup{}
	wg.Add(totalBlocks)
	for id, reader := range fs.recordReaders {
		reader.Start(ch, wg)
		fmt.Println("Reader started: readerId=", id)
	}
	wg.Wait()

	// main process computes minimum element
	fmt.Println("All readers started, prepare to sort partitions...")
	reqCache := make([]ReqValue, 0)

	// notify seekers to offer values
	cnt := 0
	for _, reader := range fs.recordReaders {
		cnt += reader.SignalToOfferValues()
	}
	fmt.Println("Signaled ", cnt, "seekers to offer values.")

	totalElements := 0
	done := false

	for {
		// collect req values
		for {
			req := <-ch
			cnt--
			reqCache = append(reqCache, req)
			if cnt == 0 {
				break
			}
		}

		// select the minimum element
		for {
			var selectedReq ReqValue
			var selectedIndex int
			minVal := MaxIntValue
			for i, req := range reqCache {
				if req.Value < minVal {
					minVal = req.Value
					selectedReq = req
					selectedIndex = i
				}
			}
			if len(reqCache) > 1 {
				reqCache = append(reqCache[:selectedIndex], reqCache[selectedIndex+1:]...)
			} else {
				reqCache = []ReqValue{}
			}

			// write the element to the sorted files
			fs.outFile.WriteString(fmt.Sprintf("%04d", minVal))
			totalElements++

			readerId := selectedReq.ReaderId
			seekerId := selectedReq.SeekerId
			if !fs.isSeekerCompleted(readerId, seekerId) {
				fs.recordReaders[readerId].SeekNextValue(seekerId)
				cnt = 1
				break
			} else {
				if len(reqCache) == 0 && fs.isSeekersCompleted() {
					done = true
					break
				} else {
					continue
				}
			}
		}
		if done {
			break
		}
	}
	fmt.Println("File sort completed: totalElements=", totalElements, ", sortedFile=", fs.OutputPath)
}

type OffsetSeeker struct {
	Id          int
	ReaderId    int
	StartOffset int64
	BlockLen    int
	DataFile    string
	Queue       chan string
	IsCompleted bool
	filePointer *os.File
	currentPos  int
}

func (s * OffsetSeeker) Initialize() {
	s.Queue = make(chan string, 1)
	// open the data file
	f, err := os.Open(s.DataFile)
	if err == nil {
		s.filePointer = f
		f.Seek(s.StartOffset, 1)
		s.currentPos = int(s.StartOffset)
	}
}

func (s * OffsetSeeker) Run(ch chan ReqValue) {
	size := BlockElementSize
	defer func() {
		s.filePointer.Close()
		close(s.Queue)
	}()
	// manage file reading for the specified block
	for {
		select {
		case flag := <-s.Queue:
		fmt.Println("Recv singal: readerId=", s.ReaderId, ", seekerId=", s.Id, ", flag=", flag)
			b := make([]byte, size)
			s.filePointer.Read(b)
			value := string(b)
			fmt.Println("Read block: readerId=", s.ReaderId, ", seekerId=", s.Id, ", startPos=", s.currentPos, ", endPos=", (s.currentPos+size), ", value=", value)
			elem, _ := strconv.Atoi(value)
		fmt.Println("Offering value: readerId=", s.ReaderId, ", seekerId=", s.Id, ", value=", elem)
			ch <- ReqValue{s.ReaderId, s.Id, elem}
			s.currentPos += size
			if s.currentPos >= int(s.StartOffset) + s.BlockLen {
				s.IsCompleted = true
			}
		default:
		}

		// seeker is completed, break the loop
		if s.IsCompleted {
			fmt.Println("Seeker done: readerId=", s.ReaderId, ", seekerId=", s.Id)
			break
		}
	}
}

func (s *OffsetSeeker) SignalToOfferNext() {
	s.Queue <- "X"
}

type FileRecordReader struct {
	Id              int
	IsAlive         bool
	Queue           chan string
	FileMeta	*fileMeta
	offsetSeekers	map[int]*OffsetSeeker
	blockCount	int
	indexFile	string
	dataFile	string
}

func NewFileRecordReader(id int, meta *fileMeta) *FileRecordReader {
	meta.Check()
	return &FileRecordReader{
		Id: id,
		FileMeta: meta,
	}
}

func (r *FileRecordReader) Initialize() {
	r.offsetSeekers = make(map[int]*OffsetSeeker)
	// index file line example:
	// 0,18362	18362,3782	22144,213
	r.indexFile = fmt.Sprintf("%s%s", filepath.Join(r.FileMeta.dir, r.FileMeta.fileName), IndexFileSuffix)
	r.dataFile = fmt.Sprintf("%s%s", filepath.Join(r.FileMeta.dir, r.FileMeta.fileName), DataFileSuffix)
	fmt.Println("Build files: readerId=", r.Id, ", files=(", r.indexFile, ", ", r.dataFile, ")")
	// parse index file
	ibytes, ierr := ioutil.ReadFile(r.indexFile)
	if ierr == nil {
		posInfos := strings.Split(string(ibytes), "\t")
		// compute block count
		r.blockCount = len(posInfos)
		// create offset seekers
		for i := 0; i < r.blockCount; i++ {
			offsetLenPair := strings.Split(posInfos[i], ",")
			start, _ := strconv.Atoi(offsetLenPair[0])
			length, _ := strconv.Atoi(offsetLenPair[1])
			seeker := &OffsetSeeker{
				ReaderId: r.Id,
				Id: i,
				StartOffset: int64(start),
				BlockLen: length,
				DataFile: r.dataFile,
				IsCompleted: false,
			}
			fmt.Println("Seeker created: readerId=", r.Id, ", seekerId=", i, ", startOffset=", start, "blockLen=", length)
			r.offsetSeekers[i] = seeker
		}
	}
}

func (r *FileRecordReader) Start(ch chan ReqValue, wg *sync.WaitGroup) {
	for seekerId, seeker := range r.offsetSeekers {
		seeker.Initialize()
		go seeker.Run(ch)
		fmt.Println("Seeker started: readerId=", r.Id, ", seekerId=", seekerId)
		wg.Done()
	}
}

func (r *FileRecordReader) SeekNextValue(seekerId int) {
	r.offsetSeekers[seekerId].SignalToOfferNext()
	fmt.Println("Singaled seeker: readerId=", r.Id, ", seekerId=", seekerId)
}

func (r *FileRecordReader) SignalToOfferValues() int {
	singledCount := 0
	for _, seeker := range r.offsetSeekers {
		if !seeker.IsCompleted {
			seeker.SignalToOfferNext()
			fmt.Println("Singaled seeker: readerId=", r.Id, ", seekerId=", seeker.Id)
			singledCount++
		}
	}
	return singledCount
}
