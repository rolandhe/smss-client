package main

import (
	"github.com/rolandhe/smss/client/client"
	"log"
	"time"
)

func main() {
	rc, err := client.NewReplicaClient("localhost", 8080, time.Second*5)
	if err != nil {
		log.Printf("%v\n", err)
		return
	}

	defer rc.Close()

	err = rc.Replica(97640)
	log.Println(err)
}
