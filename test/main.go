package main

import (
	"fmt"
	"net"
	"time"
)

func main() {
	//ch := make(chan struct{})
	//close(ch)
	//
	//_, ok := <-ch
	//fmt.Println(ok)
	_, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", "localhost", 8080), time.Second*2)
	if err != nil {
		return
	}

}
