package main

import (
	"github.com/rolandhe/smss/client/client"
	"log"
	"time"
)

func main() {
	//create()
	getMqList()
	//delete()
}

func create() {
	pc, err := client.NewPubClient("localhost", 8080, time.Second*5)
	if err != nil {
		log.Printf("%v\n", err)
		return
	}
	defer pc.Close()

	err = pc.CreateMQ("temp_mq", 0, "tid-2209991")

	log.Println(err)
}

func delete() {
	pc, err := client.NewPubClient("localhost", 8080, time.Second*500)
	if err != nil {
		log.Printf("%v\n", err)
		return
	}
	defer pc.Close()

	err = pc.DeleteMQ("temp_mq", "tid-9999del33")

	log.Println(err)
}

func getMqList() {
	pc, err := client.NewPubClient("localhost", 8080, time.Second*5)
	if err != nil {
		log.Printf("%v\n", err)
		return
	}
	defer pc.Close()

	var j string
	j, err = pc.GetMqList("tid-99yymm009")

	log.Println(j, err)
}
