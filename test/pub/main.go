package main

import (
	"fmt"
	"github.com/rolandhe/smss/smss-client/client"
	"log"
	"strconv"
	"sync"
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
	//multi(32)
}

type One struct {
	name string
}

type Two struct {
	One
}

func multi(count int) {
	start := time.Now().UnixMilli()
	var wg sync.WaitGroup
	wg.Add(count)
	notify := make(chan struct{})
	for i := 0; i < count; i++ {
		go thread(i, &wg, notify)
	}
	close(notify)
	wg.Wait()
	end := time.Now().UnixMilli()

	log.Printf("finish all threads,cost:%d ms", end-start)
}

func thread(no int, wg *sync.WaitGroup, notify chan struct{}) {
	go func() {
		pc, err := client.NewPubClient("localhost", 12301, time.Second*50000)
		if err != nil {
			log.Printf("%v\n", err)
			return
		}
		defer pc.Close()
		<-notify
		//base := "thread=%d,index=%d,ggo,Voice of America is the state-owned news network and international radio broadcaster of the United States of America.AlibabaCloud (darwin; arm64) Node.js/v16.14.2 Core/1.0.1 TeaDSL/1 cloud-assist/1.2.5--j8"
		base := "thread=%d,index=%d,CREATE INDEX idx_name_prefix ON table_name(name(10));little,Voice of America is the state-owned-j20"
		for i := 1; i <= 100000; i++ {
			body := fmt.Sprintf(base, no, i)
			msg := client.NewMessage([]byte(body))
			tid := fmt.Sprintf("tid-%d-%d", no, i)
			msg.AddHeader("traceId", tid)
			err = pc.Publish("order", msg, tid)
			if err != nil {
				log.Printf("%v\n", err)
				break
			}
			if i%10000 == 0 {
				log.Printf("finish no=%d,index=%d\n", no, i)
			}
		}
		wg.Done()
	}()
}

func pub() {
	pc, err := client.NewPubClient("localhost", 12301, time.Second*5000)
	if err != nil {
		log.Printf("%v\n", err)
		return
	}

	//base := "Voice of America is the state-owned news network and international radio broadcaster of the United States of America. It is the largest and oldest of the U.S.-funded international broadcasters. VOA produces digital, TV, and radio content in 49 languages, which it distributes to affiliate stations around the world。A licensee of an international broadcast station shall render only an international broadcast service which will reflect the culture of this country and which will promote international goodwill, understanding and cooperation. Any program solely intended for, and directed to an audience in the continental United States does not meet the requirements for this service。Even before the December 1941 Japanese attack on Pearl Harbor, the U.S. government's Office of the Coordinator of Information (COI) had already begun providing war news and commentary to the commercial American shortwave radio stations for use on a voluntary basis, through its Foreign Information Service (FIS) headed by playwright Robert E. Sherwood, who served as President Franklin Delano Roosevelt's speech writer and information advisor.[27] Direct programming began a week after the United States' entry into World War II in December 1941, with the first broadcast from the San Francisco office of the FIS via General Electric's KGEI transmitting to the Philippines in English (other languages followed). The next step was to broadcast to Germany, which was called Stimmen aus Amerika (\"Voices from America\") and was transmitted on February 1, 1942. It was introduced by the “Battle Hymn of the Republic\" and included the pledge: \"Today, and every day from now on, we will be with you from America to talk about the war... The news may be good or bad for us – We will always tell you the truth.\"[28] Roosevelt approved this broadcast, which then-Colonel William J. Donovan (COI) and Sherwood (FIS) had recommended to him. It was Sherwood who actually coined the term \"The Voice of America\" to describe the shortwave network that began its transmissions on February 1, from 270 Madison Avenue in New York City----"
	base := "ggppmm-hello world,haha."
	for i := 0; i < 100000; i++ {
		buf := []byte(base + strconv.Itoa(i))
		msg := client.NewMessage(buf)
		msg.AddHeader("traceId", fmt.Sprintf("tid-%d", i))
		err = pc.Publish("order", msg, "tid-999pxxfdb11")
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
	pc, err := client.NewPubClient("localhost", 12301, time.Second*50000)
	if err != nil {
		log.Printf("%v\n", err)
		return
	}

	i := 11

	msg := client.NewMessage([]byte("delay-test 99999-" + strconv.Itoa(i)))
	msg.AddHeader("traceId", fmt.Sprintf("tid-%d", i))
	err = pc.PublishDelay("order", msg, 10*60*1000, "tid-777777")
	log.Printf("%v\n", err)

}

//func changeLf() {
//	pc, err := client.NewPubClient("localhost", 8080, time.Second*30)
//	if err != nil {
//		log.Printf("%v\n", err)
//		return
//	}
//
//	defer pc.Close()
//
//	err = pc.ChangeMqLife("temp_mq", time.Now().Add(time.Second*10).UnixMilli(), "tid-999999")
//	log.Printf("%v\n", err)
//}
