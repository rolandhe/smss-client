package main

import (
	"github.com/rolandhe/smss-client/client"
	"github.com/rolandhe/smss-client/logger"
	"log"
	"time"
)

func main() {
	rc, err := client.NewReplicaClient("localhost", 8080, time.Second*5)
	if err != nil {
		logger.Infof("%v\n", err)
		return
	}

	defer rc.Close()

	err = rc.Replica(97640)
	log.Println(err)
}
