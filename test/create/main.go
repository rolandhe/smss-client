package main

import (
	"github.com/rolandhe/smss/smss-client/client"
	"log"
	"time"
)

func main() {
	create()
	//getMqList()
	//delete()
}

func create() {
	pc, err := client.NewPubClient("localhost", 12301, time.Second*5)
	if err != nil {
		log.Printf("%v\n", err)
		return
	}
	defer pc.Close()

	//mqName := "audience-audience-audience-audience-audience-audience-audience-audience-audience-audience-audience-audience-123abcdefg-00900000008"
	mqName := "order"

	//expireAt := time.Now().Add(time.Minute * 2).UnixMilli()
	err = pc.CreateMQ(mqName, 0, "tid-2209991")

	log.Println(err)
}

func delete() {
	pc, err := client.NewPubClient("localhost", 12301, time.Second*500)
	if err != nil {
		log.Printf("%v\n", err)
		return
	}
	defer pc.Close()

	err = pc.DeleteMQ("temp_mq", "tid-9999del33")

	log.Println(err)
}

func getMqList() {
	pc, err := client.NewPubClient("localhost", 12301, time.Second*5)
	if err != nil {
		log.Printf("%v\n", err)
		return
	}
	defer pc.Close()

	var j string
	j, err = pc.GetMqList("tid-99yymm009")

	log.Println(j, err)
}

func getValidMqList() {
	pc, err := client.NewPubClient("localhost", 12301, time.Second*5)
	if err != nil {
		log.Printf("%v\n", err)
		return
	}
	defer pc.Close()

	var j string
	j, err = pc.g("tid-99yymm009")

	log.Println(j, err)
}