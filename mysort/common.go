package mysort

const (
	MaxIntValue = int(^uint(0) >> 1)
	DataFileSuffix = ".data"
	IndexFileSuffix = ".index"
)

type Sorter interface {
	Sort(partitions []*Partition) []int
}

type Partition struct {
	Id int
	Blocks []*Block
}

type Block struct {
	Id int
	Data []int
}

type BucketInfo struct {
	Id int
	Blk *Block
	Pos int
}