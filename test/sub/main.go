package main

import (
	"flag"
	"fmt"
	"github.com/rolandhe/smss-client/client"
	redisLock "github.com/rolandhe/smss-client/dlock/redis"
	"github.com/rolandhe/smss-client/logger"
	"time"
)

var (
	who     = flag.String("who", "vvi", "subscriber who")
	eventId = flag.Int64("event", 1294734, "event id")
)

func main() {
	flag.Parse()
	fmt.Println(*who, *eventId)
	//sub(*who, *eventId)
	dlockSub(*who, *eventId)
}

func dlockSub(who string, eventId int64) {
	locker := redisLock.NewRedisSubLock("localhost", 6379, true)
	lsub := client.NewDLockSub("order", who, "localhost", 12301, time.Second*5, locker)

	count := int64(0)

	err := lsub.Sub(eventId, 5, time.Second*10, func(messages []*client.SubMessage) client.AckEnum {
		for _, msg := range messages {

			var body string
			if len(msg.GetPayload()) > 500 {
				body = string(msg.GetPayload()[len(msg.GetPayload())-500:])
			} else {
				body = string(msg.GetPayload())
			}
			if count%50 == 0 {
				logger.Infof("ts=%d, eventId=%d, fileId=%d, pos=%d, body is: %s", msg.Ts, msg.EventId, msg.FileId, msg.Pos, body)
			}
			count++
		}
		return client.Ack
	})
	logger.Infof("dlockSub err:%v", err)

	time.Sleep(time.Second * 30)
	locker.Shutdown()
}

func sub(who string, eventId int64) {
	sc, err := client.NewSubClient("order", who, "localhost", 12301, time.Second*5)
	if err != nil {
		logger.Infof("%v\n", err)
		return
	}

	defer sc.Close()

	count := int64(0)
	// 311041
	err = sc.Sub(eventId, 5, time.Second*10, func(messages []*client.SubMessage) client.AckEnum {
		for _, msg := range messages {

			var body string
			if len(msg.GetPayload()) > 500 {
				body = string(msg.GetPayload()[len(msg.GetPayload())-500:])
			} else {
				body = string(msg.GetPayload())
			}
			logger.Infof("ts=%d, eventId=%d, fileId=%d, pos=%d, body is: %s\n", msg.Ts, msg.EventId, msg.FileId, msg.Pos, body)
			count++
		}
		return client.Ack
	})
	if err != nil {
		logger.Infof("%v\n", err)
		return
	}
}

func accept(messages []*client.SubMessage) client.AckEnum {
	for _, msg := range messages {
		var body string
		if len(msg.GetPayload()) > 50 {
			body = string(msg.GetPayload()[len(msg.GetPayload())-50:])
		} else {
			body = string(msg.GetPayload())
		}
		logger.Infof("%d, %d, %d, %d: %s", msg.Ts, msg.EventId, msg.FileId, msg.Pos, body)
	}
	return client.Ack
}
