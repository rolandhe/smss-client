package main

import (
	"github.com/rolandhe/smss/smss-client/client"
	"github.com/rolandhe/smss/smss-client/test/config"
	"log"
	"time"
)

func main() {
	create("order", 0)
	//create("order2", time.Second*35)
	//create("order3", time.Second*20)
	//getTopicList()

	//getTopicInfo()
	//delete()
	poolTest()

}

func poolTest() {
	pcPool := client.NewPubClientPool(config.NewConfig(), "localhost", 12301, time.Second*5)
	pc, err := pcPool.Borrow()
	if err != nil {
		log.Println(err)
		return
	}
	var j string
	j, err = pc.GetTopicInfo("order", "tid-99yymm009")

	log.Println(j, err)
	pc.Close()

	time.Sleep(time.Second * 300)
	pcPool.ShutDown()
}

func create(topicName string, delayDuration time.Duration) {
	pc, err := client.NewPubClient("localhost", 12301, time.Second*5)
	if err != nil {
		log.Printf("%v\n", err)
		return
	}
	defer pc.Close()

	//topicName := "audience-audience-audience-audience-audience-audience-audience-audience-audience-audience-audience-audience-123abcdefg-00900000008"
	//topicName := "order2"

	expireAt := int64(0)
	if delayDuration > 0 {
		expireAt = time.Now().Add(delayDuration).UnixMilli()
	}

	err = pc.CreateTopic(topicName, expireAt, "tid-2209991")

	log.Println(err)
}

func delete() {
	pc, err := client.NewPubClient("localhost", 12301, time.Second*500)
	if err != nil {
		log.Printf("%v\n", err)
		return
	}
	defer pc.Close()

	err = pc.DeleteTopic("temp_topic", "tid-9999del33")

	log.Println(err)
}

func getTopicInfo() {
	topicName := "order33"
	pc, err := client.NewPubClient("localhost", 12301, time.Second*5)
	if err != nil {
		log.Printf("%v\n", err)
		return
	}
	defer pc.Close()

	var j string
	j, err = pc.GetTopicInfo(topicName, "tid-99yymm009")

	log.Println(j, err)
}

func getTopicList() {
	pc, err := client.NewPubClient("localhost", 12301, time.Second*5)
	if err != nil {
		log.Printf("%v\n", err)
		return
	}
	defer pc.Close()

	var j string
	j, err = pc.GetTopicList("tid-99yymm009")

	log.Println(j, err)
}

func getValidTopicList() {
	pc, err := client.NewPubClient("localhost", 12301, time.Second*5)
	if err != nil {
		log.Printf("%v\n", err)
		return
	}
	defer pc.Close()

	var j string
	j, err = pc.GetTopicList("tid-99yymm009")

	log.Println(j, err)
}
