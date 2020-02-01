package authalog

import "fmt"

var LogTrace = false

func trace(args ...interface{}) {
	if LogTrace {
		fmt.Println(args...)
	}
}
