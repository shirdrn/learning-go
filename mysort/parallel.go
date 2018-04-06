package mysort

import "fmt"

/////////////////////////
// PARALLEL IN-MEM SORT
////////////////////////

type ParallelSorter struct {

}

type ReqElem struct {
	WorkerId int
	Value int
}

func (ps *ParallelSorter) Sort(partitions []*Partition) []int {
	pLen := len(partitions)
	workers := make(map[int]*PartitionWorker, 0)
	ch := make(chan ReqElem, pLen)
	fmt.Println("pLen = ", pLen)
	for i := 0; i < pLen; i ++ {
		pw := NewWorker(i, partitions[i])
		pw.Initialize()
		workers[pw.Id] = pw
		fmt.Println("workers = ", workers)
		go pw.PickUp(ch)
	}
	return ps.control(ch, workers)
}

func (ps *ParallelSorter) control(ch chan ReqElem, workers map[int]*PartitionWorker) []int {
	a := make([]int, 0)
	// main controller
	round := 0
	for {
		minVal := MaxIntValue
		// parallel to pick up
		fmt.Println(":::::: Round (", round, ") ::::::" )
		round++
		cnt := 0
		for id, worker := range workers {
			fmt.Println("workerId = ", id, ", isAlive = ", worker.IsAlive)
			// notify workers to pick up an element
			if worker.IsAlive {
				worker.Queue <- "X"
				cnt++
				fmt.Println("cnt=", cnt)
			}
		}

		if cnt == 0 {
			close(ch)
			break
		}

		// select the minimum element
		var thisReq ReqElem
		for {
			req := <-ch
			cnt--
			fmt.Println("Control-> compare: element=", req.Value, ", minVal=", minVal)
			if req.Value < minVal {
				thisReq = req
				minVal = req.Value
			}
			if cnt == 0 {
				a = append(a, minVal)
				fmt.Println("Sorted area snapshot: ", a)
				workers[thisReq.WorkerId].IncrOffset()
				break
			}
		}
		//time.Sleep(1 * time.Second)
	}
	return a
}

type PartitionWorker struct {
	Id              int
	IsAlive         bool
	Queue           chan string
	partition       *Partition
	bucketInfoMap   map[int]*BucketInfo
	lenMap          map[int]int
	currentBucketId int
	currentBucket   *BucketInfo
}

func NewWorker(id int, partition *Partition) *PartitionWorker {
	return &PartitionWorker{
		Id: id,
		IsAlive: true,
		Queue: make(chan string, 1),
		partition: partition,
	}
}

func (w *PartitionWorker) Initialize() {
	w.bucketInfoMap = make(map[int]*BucketInfo)
	w.lenMap = make(map[int]int)
	for _, blk := range w.partition.Blocks {
		w.lenMap[blk.Id] = len(blk.Data)
		w.bucketInfoMap[blk.Id] = &BucketInfo{blk.Id, blk, 0}
		fmt.Println("j=", blk.Id, ", blk=", blk)
	}
}

func (w *PartitionWorker) PickUp(ch chan ReqElem) {
	fmt.Println("PickUp enter: workerId = ", w.Id)
	for {
		select {
		case flag := <- w.Queue:
			fmt.Println("PickUp: workerId = ", w.Id, ", flag = ", flag)
			var boundReachedCounter = len(w.lenMap)
			var minVal = MaxIntValue
		// iterate elements based on bucket info map
			for bucketId, bi := range w.bucketInfoMap {
				if bi.Pos < w.lenMap[bucketId] {
					value := bi.Blk.Data[bi.Pos]
					fmt.Println("Get: workerId = ", w.Id, ", value = ", value)
					if value < minVal {
						minVal = value
						w.currentBucketId = bucketId
						w.currentBucket = bi
					}
				} else {
					boundReachedCounter--
					if boundReachedCounter == 0 {
						break
					}
				}
			}
			if minVal != MaxIntValue {
				fmt.Println("Pickup: workerId = ", w.Id, ", value = ", minVal)
				ch <- ReqElem{w.Id, minVal}
			}
		default:
		}

		if !w.IsAlive {
			break
		}
	}
}

func (w *PartitionWorker) IncrOffset() {
	if w.currentBucket.Pos <= w.lenMap[w.currentBucketId] - 1 {
		w.currentBucket.Pos = 1 + w.currentBucket.Pos
	}

	// check worker status
	done := true
	for _, bi := range w.bucketInfoMap {
		if bi.Pos != len(bi.Blk.Data) {
			done = false
			break
		}
	}

	fmt.Println("IncrOffset: workerId=", w.Id, ", pos = ", w.currentBucket.Pos)

	if done {
		w.IsAlive = false
		close(w.Queue)
	}
}