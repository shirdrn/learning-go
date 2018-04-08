package main

import (
	"fmt"
	"../learning-go/mysort"
)

var blocks1 = []*mysort.Block{
	{0, []int{1, 9, 38} }, {1, []int{2, 10, 18, 27} },
	{3, []int{3, 11, 19, 63, 66} }, {4, []int{4, 12, 20} },
}
var blocks2 = []*mysort.Block{
	{5, []int{5, 13, 21} }, {6, []int{6, 14, 22} },
	{2, []int{7, 15, 23} }, {8, []int{16, 24, 29, 43} },
}
var partitions = []*mysort.Partition{
	{0, blocks1}, {1, blocks2},
}

func InMemSort() []int {
	sorter := &mysort.InMemSorter{}
	return sorter.Sort(partitions)
}

func ParallelSort() []int {
	parallelSorter := &mysort.ParallelSorter{}
	return parallelSorter.Sort(partitions)
}

func FileSort() {
	// File sort
	//
	// 2 example partition files (part-00000, part-00001):
	// part-00000.index
	//    0,12	12,16	28,20	48,12
	// part-00000.data
	//    000100090038000200100018002700030011001900630066000400120020
	// part-00001.index
	//    0,12	12,12	24,12	36,16
	// part-00001.data
	//    0005001300210006001400220007001500230016002400290043
	//
	// Sort result:
	//    0001000200030004000500060007000900100011001200130014001500160018001900200021002200230024002700290038004300630066
	partitionFiles := []string{"part-00000", "part-00001"}
	inputDir := "/Users/yanjun/Workspaces/go-workspace/learning-go/mysort/data/"
	outputDir := "/Users/yanjun/Workspaces/go-workspace/learning-go/mysort/output/sorted_file.txt"
	fileSorter := &mysort.FileSort{
		RootDir: inputDir,
		OutputPath: outputDir,
	}
	fileSorter.Sort(partitionFiles)
}

func PrintResult(sortedResult []int) {
	for _, x := range sortedResult {
		fmt.Print(x, " ")
	}
}

func PrintHeader(content string) {
	fmt.Println("=============================")
	fmt.Println("  |		", content, "		|")
	fmt.Println("=============================")
}

func main() {
	// in-mem sorter
	PrintHeader("IN MEM SORT")
	var result = InMemSort()
	PrintResult(result)
	fmt.Println()

	// parallel sorter
	PrintHeader("PARALLEL SORT")
	result = ParallelSort()
	PrintResult(result)
	fmt.Println()

	// file sorter
	PrintHeader("FILE SORT")
	FileSort()
	fmt.Println()
}