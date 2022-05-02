package main

import (
	"fmt"
	"os"
	"strconv"
	"time"
	"6.824/mr"
)

func main() {

	m := mr.MakeCoordinator(nil, 10)
	fmt.Println(strconv.Itoa(os.Getuid()))
	for m.Done() == false {
		time.Sleep(time.Second)
	}
}
