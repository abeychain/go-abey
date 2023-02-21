package les

import (
	"fmt"
	"testing"
)

func Test_01(t *testing.T) {
	begin, end, id := LesFirstEpoch()
	fmt.Println(begin, end, id)
	height := uint64(9100000)
	begin, end, id = LesEpochFromHeight(height)
	fmt.Println(begin, end, id)
}

func Test_02(t *testing.T) {
	for i := uint64(0); i < 100; i++ {
		begin, end := LesEpochToHeight(i)
		fmt.Println("epoch", i, "begin", begin, "end", end)
	}
}
