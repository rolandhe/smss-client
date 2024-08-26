package pool

import (
	"log"
	"time"
)

type ObjPool[T any] interface {
	Borrow() (*T, error)
	Return(ins *T, bad bool) error
	ShutDown()
}

type ObjectFactory[T any] interface {
	Create() (*T, error)
	Destroy(ins *T)
	Valid(ins *T) error
}

func NewDefaultConfig() *Config {
	return &Config{
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

type Config struct {
	TestOnBorrow bool
	TestOnReturn bool

	WaitTimeout time.Duration

	BackCheck     bool
	TestOnCheck   bool
	CheckInterval time.Duration
	FirstCheck    time.Duration

	MaxSize int
	MinSize int

	MaxLifetime int

	LogFunc  func(format string, v ...any)
	LogDebug bool
}
