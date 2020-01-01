package funcp

import "strings"

func Reduce(f func(string) uint32, xs []string) string {
	ys := ""
	for _, x := range xs {
		ys += ":" + string(f(x))
	}
	return ys
}

func Find(xs []string, in string) bool {
	found := false
	for _, x := range xs {
		if strings.Contains(x, in) || strings.Contains(in, x) {
			found = true
			break
		}
	}
	return found
}
