package main

import (
	"fmt"
	"github.com/rolandhe/smss/client/client"
	"log"
	"strconv"
	"time"
)

func main() {

	//var p any
	//p = &Two{
	//	One: One{
	//		name: "aa",
	//	},
	//}
	//_, ok := p.(*Two)
	//log.Println(ok)
	pub()
	//delay()
	//changeLf()
}

type One struct {
	name string
}

type Two struct {
	One
}

func pub() {
	pc, err := client.NewPubClient("localhost", 8080, time.Second*5000)
	if err != nil {
		log.Printf("%v\n", err)
		return
	}

	for i := 0; i < 1; i++ {
		buf := []byte("100-hello world-" + strconv.Itoa(i))
		msg := client.NewMessage(buf)
		msg.AddHeader("traceId", fmt.Sprintf("tid-%d", i))
		err = pc.Publish("temp_mq", msg, "tid-999pxxfdb11")
		if err != nil {
			log.Printf("%v\n", err)
			break
		}
		if i%50 == 0 {
			log.Printf("finish %d\n", i)
		}

		//log.Println("next")
	}
}

func delay() {
	pc, err := client.NewPubClient("localhost", 8080, time.Second*5000)
	if err != nil {
		log.Printf("%v\n", err)
		return
	}

	i := 11

	msg := client.NewMessage([]byte("delay-hello world-" + strconv.Itoa(i)))
	msg.AddHeader("traceId", fmt.Sprintf("tid-%d", i))
	err = pc.PublishDelay("order", msg, 20*1000, "tid-777777")
	log.Printf("%v\n", err)

}

func changeLf() {
	pc, err := client.NewPubClient("localhost", 8080, time.Second*30)
	if err != nil {
		log.Printf("%v\n", err)
		return
	}

	defer pc.Close()

	err = pc.ChangeMqLife("temp_mq", time.Now().Add(time.Second*10).UnixMilli(), "tid-999999")
	log.Printf("%v\n", err)
}
