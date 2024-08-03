package utils

import (
	"fmt"
	"slices"
	"testing"
)

func TestCommaListToArray(t *testing.T) {
	arrStr := ""

	arr := CommaListToArray(arrStr)
	fmt.Println(slices.Contains(arr, "1"))
}
