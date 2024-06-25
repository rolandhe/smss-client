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
	sc, err := client.NewSubClient("temp_mq", "test_usser", "localhost", 8080, time.Second*5)
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

	err = sc.Sub(0, 0, 5, time.Second*10, accept)
	if err != nil {
		log.Printf("%v\n", err)
		return
	}
	sc.Close()
}

func accept(messages []*client.SubMessage) client.AckEnum {
	for _, msg := range messages {
		log.Printf("%d, %d, %d, %d: %s\n", msg.Ts, msg.Id, msg.FileId, msg.Pos, string(msg.GetPayload()))
	}
	return client.Ack
}
