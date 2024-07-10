package main

import (
	"flag"
	"fmt"
	"github.com/rolandhe/smss/smss-client/client"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var (
	who     = flag.String("who", "", "subscriber who")
	eventId = flag.Int64("event", 0, "event id")
)

func main() {
	flag.Parse()
	fmt.Println(*who, *eventId)
	sub(*who, *eventId)
}

func sub(who string, eventId int64) {
	sc, err := client.NewSubClient("order", who, "localhost", 12301, time.Second*5)
	if err != nil {
		log.Printf("%v\n", err)
		return
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	//go func() {
	//	<-sigs
	//	fmt.Println("Received interrupt signal, shutting down...")
	//	sc.Close()
	//	os.Exit(0)
	//}()

	defer sc.Close()
	// 510125730

	count := int64(0)
	// 311041
	err = sc.Sub(eventId, 5, time.Second*10, func(messages []*client.SubMessage) client.AckEnum {
		for _, msg := range messages {
			//if count%10 != 0 {
			//	count++
			//	continue
			//}
			var body string
			if len(msg.GetPayload()) > 50 {
				body = string(msg.GetPayload()[len(msg.GetPayload())-50:])
			} else {
				body = string(msg.GetPayload())
			}
			if count > 25599900 || count%100000 == 0 {
				log.Printf("ts=%d, eventId=%d, fileId=%d, pos=%d, body is: %s\n", msg.Ts, msg.Id, msg.FileId, msg.Pos, body)
			}
			count++
			if count == 25600000 {
				count = 0
			}
		}
		return client.Ack
	})
	if err != nil {
		log.Printf("%v\n", err)
		return
	}
	sc.Close()
}

func accept(messages []*client.SubMessage) client.AckEnum {
	for _, msg := range messages {
		var body string
		if len(msg.GetPayload()) > 50 {
			body = string(msg.GetPayload()[len(msg.GetPayload())-50:])
		} else {
			body = string(msg.GetPayload())
		}
		log.Printf("%d, %d, %d, %d: %s\n", msg.Ts, msg.Id, msg.FileId, msg.Pos, body)
	}
	return client.Ack
}
