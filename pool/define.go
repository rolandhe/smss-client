package pool

import "time"

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
