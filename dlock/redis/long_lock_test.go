package redisLock

import (
	"github.com/rolandhe/smss/smss-client/dlock"
	"testing"
	"time"
)

func TestLock(t *testing.T) {
	locker := NewRedisLock("localhost", 6379, true)
	watchChan, err := locker.LockWatcher("dddong-ling")
	if err != nil {
		t.Error(err)
		return
	}
	for {
		var st dlock.WatchState
		select {
		case st = <-watchChan:
		case <-time.After(time.Second * 75):
			t.Logf("watch timeout 75s")
			continue
		}
		t.Logf("%v\n", st)
	}
}

func TestLock2(t *testing.T) {
	locker := NewRedisLock("localhost", 6379, true)
	watchChan, err := locker.LockWatcher("dddong-ling")
	if err != nil {
		t.Error(err)
		return
	}
	for {
		var st dlock.WatchState
		select {
		case st = <-watchChan:
		case <-time.After(time.Second * 45):
			t.Logf("watch timeout 45s")
			continue
		}
		t.Logf("%v\n", st)
	}
}
