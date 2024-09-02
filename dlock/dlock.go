package dlock

type WatchState int

const (
	Locked WatchState = 1
	Leased WatchState = 2

	LostLock    WatchState = 3
	LockTimeout WatchState = 4
)

type DLock interface {
	LockWatcher(key string) (<-chan WatchState, error)
}
