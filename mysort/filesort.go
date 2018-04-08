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
	DataFileSuffix = ".data"
	IndexFileSuffix = ".index"
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

type BlockValue struct {
	ManagerId int
	ReaderId  int
	Value     int
}

/**
  Sort block elements based on file storage.

  We assume:
  1. A single file contains a group of data blocks.
  2. data storage structure:
  partition file suffix with '.data', block index files suffix with '.index', such as
    part-00000.data  part-00000.index
    part-00001.data  part-00001.index
  We can locate each block data.
 */
type FileSort struct {
	RootDir           string
	OutputPath        string
	outFile           *os.File
	partitionManagers map[int]*PartitionManager
}

func (fs *FileSort) isReaderCompleted(managerId, readerId int) bool {
	return fs.partitionManagers[managerId].blockReaders[readerId].IsCompleted
}

func (fs *FileSort) isAllReadersCompleted() bool {
	for _, manager := range fs.partitionManagers {
		for _, reader := range manager.blockReaders {
			if !reader.IsCompleted {
				return false
			}
		}
	}
	return true
}

func (fs *FileSort) Sort(partitionFiles []string) {
	fs.partitionManagers = make(map[int]*PartitionManager)

	// create file partition managers
	totalBlocks := 0
	for i, file := range partitionFiles {
		meta := NewFileMeta(i, fs.RootDir, file)
		manager := NewPartitionManager(i, meta)
		manager.Initialize()
		managerId := i
		fs.partitionManagers[managerId] = manager
		totalBlocks += manager.blockCount
		fmt.Println("Partition manager created: managerId=", managerId, ", blockCount=", manager.blockCount)
	}
	fmt.Println("Partition manager info: managerCount=", len(fs.partitionManagers), ", totalBlocks=", totalBlocks)
	ch := make(chan BlockValue, totalBlocks)
	defer close(ch)

	// open output file for writing sorted results
	fs.outFile, _ = os.Create(fs.OutputPath)
	defer fs.outFile.Close()

	// start file partition managers
	wg := &sync.WaitGroup{}
	wg.Add(totalBlocks)
	for id, manager := range fs.partitionManagers {
		manager.Start(ch, wg)
		fmt.Println("Partition manager started: managerId=", id)
	}
	wg.Wait()

	// main process computes minimum element
	fmt.Println("All partition manager started, prepare to sort partitions...")
	reqBlockValueBuffer := make([]BlockValue, 0)

	// notify block readers to read & offer values
	cnt := 0
	for _, manager := range fs.partitionManagers {
		cnt += manager.SignalToOfferValues()
	}
	fmt.Println("Signaled ", cnt, "readers to offer block values.")

	totalElements := 0
	done := false

	for {
		// collect req values
		for {
			req := <-ch
			cnt--
			reqBlockValueBuffer = append(reqBlockValueBuffer, req)
			if cnt == 0 {
				break
			}
		}

		// select the minimum element
		for {
			var selectedReq BlockValue
			var selectedIndex int
			minVal := MaxIntValue
			for i, req := range reqBlockValueBuffer {
				if req.Value < minVal {
					minVal = req.Value
					selectedReq = req
					selectedIndex = i
				}
			}
			if len(reqBlockValueBuffer) > 1 {
				reqBlockValueBuffer = append(reqBlockValueBuffer[:selectedIndex], reqBlockValueBuffer[selectedIndex+1:]...)
			} else {
				reqBlockValueBuffer = []BlockValue{}
			}

			// write the element to the sorted files
			fs.outFile.WriteString(fmt.Sprintf("%04d", minVal))
			totalElements++

			managerId := selectedReq.ManagerId
			readerId := selectedReq.ReaderId
			if !fs.isReaderCompleted(managerId, readerId) {
				fs.partitionManagers[managerId].ReadNextValue(readerId)
				cnt = 1
				break
			} else {
				if len(reqBlockValueBuffer) == 0 && fs.isAllReadersCompleted() {
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

type BlockReader struct {
	Id          int
	ManagerId   int
	StartOffset int64
	BlockLen    int
	DataFile    string
	Queue       chan string
	IsCompleted bool
	filePointer *os.File
	currentPos  int
}

func (br *BlockReader) Initialize() {
	br.Queue = make(chan string, 1)
	// open the data file
	f, err := os.Open(br.DataFile)
	if err == nil {
		br.filePointer = f
		f.Seek(br.StartOffset, 1)
		br.currentPos = int(br.StartOffset)
	}
}

func (br *BlockReader) Run(ch chan BlockValue) {
	size := BlockElementSize
	defer func() {
		br.filePointer.Close()
		close(br.Queue)
	}()
	// manage block element reading for the specified block
	for {
		select {
		case flag := <-br.Queue:
			fmt.Println("Recv singal: managerId=", br.ManagerId, ", readerId=", br.Id, ", flag=", flag)
			b := make([]byte, size)
			br.filePointer.Read(b)
			value := string(b)
			fmt.Println("Read block: managerId=", br.ManagerId, ", readerId=", br.Id, ", startPos=", br.currentPos, ", endPos=", (br.currentPos+size), ", value=", value)
			elem, _ := strconv.Atoi(value)
			fmt.Println("Offering value: managerId=", br.ManagerId, ", readerId=", br.Id, ", value=", elem)

			ch <- BlockValue{br.ManagerId, br.Id, elem}
			br.currentPos += size
			if br.currentPos >= int(br.StartOffset) + br.BlockLen {
				br.IsCompleted = true
			}
		default:
		}

		// block reader is completed, break the loop
		if br.IsCompleted {
			fmt.Println("Block reader done: managerId=", br.ManagerId, ", readerId=", br.Id)
			break
		}
	}
}

func (br *BlockReader) SignalToOfferNext() {
	br.Queue <- "X"
}

type PartitionManager struct {
	Id           int
	IsAlive      bool
	Queue        chan string
	FileMeta     *fileMeta
	blockReaders map[int]*BlockReader
	blockCount   int
	indexFile    string
	dataFile     string
}

func NewPartitionManager(id int, meta *fileMeta) *PartitionManager {
	meta.Check()
	return &PartitionManager{
		Id: id,
		FileMeta: meta,
	}
}

func (pm *PartitionManager) Initialize() {
	pm.blockReaders = make(map[int]*BlockReader)
	// index file line example:
	// 0,18362	18362,3782	22144,213
	pm.indexFile = fmt.Sprintf("%s%s", filepath.Join(pm.FileMeta.dir, pm.FileMeta.fileName), IndexFileSuffix)
	pm.dataFile = fmt.Sprintf("%s%s", filepath.Join(pm.FileMeta.dir, pm.FileMeta.fileName), DataFileSuffix)
	fmt.Println("Build files: managerId=", pm.Id, ", files=(", pm.indexFile, ", ", pm.dataFile, ")")
	// parse index file
	ibytes, ierr := ioutil.ReadFile(pm.indexFile)
	if ierr == nil {
		posInfos := strings.Split(string(ibytes), "\t")
		// compute block count
		pm.blockCount = len(posInfos)
		// create block readers
		for i := 0; i < pm.blockCount; i++ {
			offsetLenPair := strings.Split(posInfos[i], ",")
			start, _ := strconv.Atoi(offsetLenPair[0])
			length, _ := strconv.Atoi(offsetLenPair[1])
			reader := &BlockReader{
				ManagerId: pm.Id,
				Id: i,
				StartOffset: int64(start),
				BlockLen: length,
				DataFile: pm.dataFile,
				IsCompleted: false,
			}
			fmt.Println("Block reader created: managerId=", pm.Id, ", readerId=", i, ", startOffset=", start, "blockLen=", length)
			pm.blockReaders[i] = reader
		}
	}
}

func (pgm *PartitionManager) Start(ch chan BlockValue, wg *sync.WaitGroup) {
	for readerId, reader := range pgm.blockReaders {
		reader.Initialize()
		go reader.Run(ch)
		fmt.Println("Block reader started: managerId=", pgm.Id, ", readerId=", readerId)
		wg.Done()
	}
}

func (pm *PartitionManager) ReadNextValue(readerId int) {
	pm.blockReaders[readerId].SignalToOfferNext()
	fmt.Println("Singaled block reader: managerId=", pm.Id, ", readerId=", readerId)
}

func (pm *PartitionManager) SignalToOfferValues() int {
	singledCount := 0
	for _, reader := range pm.blockReaders {
		if !reader.IsCompleted {
			reader.SignalToOfferNext()
			fmt.Println("Singaled block reader: managerId=", pm.Id, ", readerId=", reader.Id)
			singledCount++
		}
	}
	return singledCount
}
