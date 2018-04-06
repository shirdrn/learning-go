package mysort

import (
	"fmt"
)

/////////////////////////
// SIMPLE IN-MEM SORT
////////////////////////

type InMemSorter struct {

}

func (ims *InMemSorter) Sort(partitions []*Partition) []int {
	a := make([]int, 0)
	bucketInfoMap := make(map[int]*BucketInfo)
	lenMap := make(map[int]int)

	// record block length & first element data
	for _, p := range partitions {
		for _, blk := range p.Blocks {
			lenMap[blk.Id] = len(blk.Data)
			bucketInfoMap[blk.Id] = &BucketInfo{blk.Id, blk, 0}
			fmt.Println("j=", blk.Id, ", blk=", blk)
		}
	}

	for {
		var minVal = MaxIntValue
		var selectedBucketId = 0
		var theBucketInfo *BucketInfo
		var boundReachedCounter = len(lenMap)

		// iterate elements based on bucket info map
		for bucketId, bi := range bucketInfoMap {
			if bi.Pos < lenMap[bucketId] {
				value := bi.Blk.Data[bi.Pos]
				if value < minVal {
					minVal = value
					selectedBucketId = bucketId
					theBucketInfo = bi
				}
			} else {
				boundReachedCounter--
				if boundReachedCounter == 0 {
					break
				}
			}
		}

		// whether append a sorted element to the ordered area
		if minVal != MaxIntValue {
			a = append(a, minVal)
			fmt.Println(a)
			if theBucketInfo.Pos <= lenMap[selectedBucketId] - 1 {
				theBucketInfo.Pos = 1 + theBucketInfo.Pos
			}
		} else {
			break
		}
	}
	return a
}