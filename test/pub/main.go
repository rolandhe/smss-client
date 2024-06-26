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

	//base := "Voice of America is the state-owned news network and international radio broadcaster of the United States of America. It is the largest and oldest of the U.S.-funded international broadcasters. VOA produces digital, TV, and radio content in 49 languages, which it distributes to affiliate stations around the world。A licensee of an international broadcast station shall render only an international broadcast service which will reflect the culture of this country and which will promote international goodwill, understanding and cooperation. Any program solely intended for, and directed to an audience in the continental United States does not meet the requirements for this service。Even before the December 1941 Japanese attack on Pearl Harbor, the U.S. government's Office of the Coordinator of Information (COI) had already begun providing war news and commentary to the commercial American shortwave radio stations for use on a voluntary basis, through its Foreign Information Service (FIS) headed by playwright Robert E. Sherwood, who served as President Franklin Delano Roosevelt's speech writer and information advisor.[27] Direct programming began a week after the United States' entry into World War II in December 1941, with the first broadcast from the San Francisco office of the FIS via General Electric's KGEI transmitting to the Philippines in English (other languages followed). The next step was to broadcast to Germany, which was called Stimmen aus Amerika (\"Voices from America\") and was transmitted on February 1, 1942. It was introduced by the “Battle Hymn of the Republic\" and included the pledge: \"Today, and every day from now on, we will be with you from America to talk about the war... The news may be good or bad for us – We will always tell you the truth.\"[28] Roosevelt approved this broadcast, which then-Colonel William J. Donovan (COI) and Sherwood (FIS) had recommended to him. It was Sherwood who actually coined the term \"The Voice of America\" to describe the shortwave network that began its transmissions on February 1, from 270 Madison Avenue in New York City----"
	base := "x-hello world,haha."
	for i := 0; i <= 300; i++ {
		buf := []byte(base + strconv.Itoa(i))
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
