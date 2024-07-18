package main

import (
	"github.com/rolandhe/smss/smss-client/client"
	"log"
	"time"
)

func main() {
	create()
	//getTopicList()
	//delete()
}

func create() {
	pc, err := client.NewPubClient("localhost", 12301, time.Second*5)
	if err != nil {
		log.Printf("%v\n", err)
		return
	}
	defer pc.Close()

	//topicName := "audience-audience-audience-audience-audience-audience-audience-audience-audience-audience-audience-audience-123abcdefg-00900000008"
	topicName := "order"

	//expireAt := time.Now().Add(time.Minute * 2).UnixMilli()
	err = pc.CreateTopic(topicName, 0, "tid-2209991")

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
