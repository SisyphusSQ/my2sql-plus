package utils

import (
	"github.com/SisyphusSQ/my2sql/internal/log"
	"slices"
	"strings"

	"github.com/spf13/cast"
)

func SliceToString[S ~[]E, E comparable](arr S, sep, prefix string) string {
	return prefix + " " + strings.Join(cast.ToStringSlice(arr), sep)
}

func CommaListToArray(str string) []string {
	arr := make([]string, 0)
	for _, item := range strings.Split(str, ",") {
		item = strings.TrimSpace(item)

		if item != "" {
			arr = append(arr, item)
		}
	}

	return arr
}

func CheckItemInSlice[S ~[]E, E comparable](arr S, element E, prefix string, ifExt bool) bool {
	if slices.Contains(arr, element) {
		return true
	} else {
		if ifExt {
			log.Logger.Fatal("%s, %s", prefix, SliceToString(cast.ToStringSlice(arr), ",", "valid args are: "))
		}
		return false
	}
}

func ConvertToSliceAny[S ~[]E, E comparable](ss S) []any {
	anys := make([]any, 0, len(ss))
	for _, s := range ss {
		anys = append(anys, s)
	}
	return anys
}
