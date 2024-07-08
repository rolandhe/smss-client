package main

import (
	"github.com/rolandhe/smss/client/client"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	sub()
}

func sub() {
	sc, err := client.NewSubClient("order", "test_usser", "localhost", 12302, time.Second*5)
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
	err = sc.Sub(0, 5, time.Second*10, func(messages []*client.SubMessage) client.AckEnum {
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
			log.Printf("ts=%d, id=%d, fileId=%d, pos=%d, body is: %s\n", msg.Ts, msg.Id, msg.FileId, msg.Pos, body)
			count++
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
