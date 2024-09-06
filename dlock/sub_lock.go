package dlock

type WatchState int

const (
	Locked WatchState = 1
	Leased WatchState = 2

	LostLock       WatchState = 3
	LockTimeout    WatchState = 4
	LockerShutdown WatchState = 5
)

type TermiteLockChan chan<- struct{}

func (tc TermiteLockChan) Termite() {
	close(tc)
}

type SubLock interface {
	LockWatcher(key string, watcherFunc func(event WatchState, termiteChan TermiteLockChan)) error
	Shutdown()
}
