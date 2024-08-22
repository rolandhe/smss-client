package config

import (
	"github.com/rolandhe/smss/smss-client/pool"
	"log"
	"time"
)

func NewConfig() *pool.Config {
	return &pool.Config{
		TestOnBorrow: true,
		TestOnReturn: true,

		WaitTimeout: time.Second * 5,

		BackCheck:     true,
		TestOnCheck:   true,
		CheckInterval: time.Second * 15,

		MaxSize: 20,
		MinSize: 5,

		MaxLifetime: 1000 * 60,

		LogFunc: func(format string, v ...any) {
			log.Printf(format, v...)
		},
		LogDebug: true,
	}
}
