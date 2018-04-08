package mysort

import "fmt"

/////////////////////////
// PARALLEL IN-MEM SORT
////////////////////////

type ParallelSorter struct {

}

type ReqElem struct {
	PickerId int
	Value    int
}

func (ps *ParallelSorter) Sort(partitions []*Partition) []int {
	pLen := len(partitions)
	pickers := make(map[int]*BlockElementPicker, 0)
	ch := make(chan ReqElem, pLen)
	fmt.Println("pLen = ", pLen)
	for i := 0; i < pLen; i ++ {
		pw := NewPartitionElementPicker(i, partitions[i])
		pw.Initialize()
		pickers[pw.Id] = pw
		fmt.Println("pickers = ", pickers)
		go pw.PickUp(ch)
	}
	return ps.control(ch, pickers)
}

func (ps *ParallelSorter) control(ch chan ReqElem, pickers map[int]*BlockElementPicker) []int {
	a := make([]int, 0)
	// main controller
	round := 0
	for {
		minVal := MaxIntValue
		// parallel to pick up
		fmt.Println(":::::: Round (", round, ") ::::::" )
		round++
		cnt := 0
		for id, picker := range pickers {
			fmt.Println("pickerId = ", id, ", isAlive = ", picker.IsAlive)
			// notify pickers to pick up an element
			if picker.IsAlive {
				picker.Queue <- "X"
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
				pickers[thisReq.PickerId].IncrOffset()
				break
			}
		}
		//time.Sleep(1 * time.Second)
	}
	return a
}

type BlockElementPicker struct {
	Id              int
	IsAlive         bool
	Queue           chan string
	partition       *Partition
	bucketInfoMap   map[int]*BucketInfo
	lenMap          map[int]int
	currentBucketId int
	currentBucket   *BucketInfo
}

func NewPartitionElementPicker(id int, partition *Partition) *BlockElementPicker {
	return &BlockElementPicker{
		Id: id,
		IsAlive: true,
		Queue: make(chan string, 1),
		partition: partition,
	}
}

func (p *BlockElementPicker) Initialize() {
	p.bucketInfoMap = make(map[int]*BucketInfo)
	p.lenMap = make(map[int]int)
	for _, blk := range p.partition.Blocks {
		p.lenMap[blk.Id] = len(blk.Data)
		p.bucketInfoMap[blk.Id] = &BucketInfo{blk.Id, blk, 0}
		fmt.Println("j=", blk.Id, ", blk=", blk)
	}
}

func (p *BlockElementPicker) PickUp(ch chan ReqElem) {
	fmt.Println("PickUp enter: pickerId = ", p.Id)
	for {
		select {
		case flag := <- p.Queue:
			fmt.Println("PickUp: pickerId = ", p.Id, ", flag = ", flag)
			var boundReachedCounter = len(p.lenMap)
			var minVal = MaxIntValue
			// iterate elements based on bucket info map
			for bucketId, bi := range p.bucketInfoMap {
				if bi.Pos < p.lenMap[bucketId] {
					value := bi.Blk.Data[bi.Pos]
					fmt.Println("Get: pickerId = ", p.Id, ", value = ", value)
					if value < minVal {
						minVal = value
						p.currentBucketId = bucketId
						p.currentBucket = bi
					}
				} else {
					boundReachedCounter--
					if boundReachedCounter == 0 {
						break
					}
				}
			}
			if minVal != MaxIntValue {
				fmt.Println("Pickup: pickerId = ", p.Id, ", value = ", minVal)
				ch <- ReqElem{p.Id, minVal}
			}
		default:
		}

		if !p.IsAlive {
			break
		}
	}
}

func (p *BlockElementPicker) IncrOffset() {
	if p.currentBucket.Pos <= p.lenMap[p.currentBucketId] - 1 {
		p.currentBucket.Pos = 1 + p.currentBucket.Pos
	}

	// check pickers status
	done := true
	for _, bi := range p.bucketInfoMap {
		if bi.Pos != len(bi.Blk.Data) {
			done = false
			break
		}
	}

	fmt.Println("IncrOffset: pickerId=", p.Id, ", pos = ", p.currentBucket.Pos)

	if done {
		p.IsAlive = false
		close(p.Queue)
	}
}