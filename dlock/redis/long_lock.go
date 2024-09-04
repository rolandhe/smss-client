package redisLock

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"github.com/rolandhe/smss/smss-client/dlock"
	"time"
)

const (
	lockedLife     = time.Second * 30
	tryLockTimeout = time.Second * 10
	leaseInterval  = time.Second * 25
)

const luaExtendScript = `
    if redis.call("get", KEYS[1]) == ARGV[1] then
        return redis.call("expire", KEYS[1], ARGV[2])
    else
        return 0
    end
`

//const luaReleaseScript = `
//        if redis.call("get", KEYS[1]) == ARGV[1] then
//            return redis.call("del", KEYS[1])
//        else
//            return 0
//        end
//    `

type rLock struct {
	host string
	port int

	rClient       *redis.Client
	notSupportLua bool
}

func NewRedisLock(host string, port int, notSupportLua bool) dlock.DLock {
	rClient := redis.NewClient(&redis.Options{
		Addr:     fmt.Sprintf("%s:%d", host, port),
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	return &rLock{host: host, port: port, rClient: rClient, notSupportLua: notSupportLua}
}

func (r *rLock) LockWatcher(key string) (<-chan dlock.WatchState, error) {
	st := &state{
		watchChan: make(chan dlock.WatchState, 10),
		key:       key,
		value:     uuid.New().String(),
	}

	go r.lock(st)

	return st.watchChan, nil
}

//func (r *rLock) release(key string, value string) bool {
//	cmd := r.rClient.Eval(context.Background(), luaReleaseScript, []string{key}, value)
//	return cmd.Val().(int64) == 1
//}

func (r *rLock) lock(st *state) {
	sm := 0
	for {
		if sm == 0 {
			ok := r.rClient.SetNX(context.Background(), st.key, st.value, lockedLife).Val()
			timeout := tryLockTimeout
			if ok {
				timeout = leaseInterval
				st.watchChan <- dlock.Locked
				sm = 1
			} else {
				st.watchChan <- dlock.LockTimeout
			}
			time.Sleep(timeout)
			continue
		}
		if sm == 1 {
			timeout := leaseInterval
			if !r.lease(st.key, st.value) {
				st.watchChan <- dlock.LostLock
				timeout = tryLockTimeout
				sm = 0
			} else {
				st.watchChan <- dlock.Leased
			}
			time.Sleep(timeout)
			continue
		}
	}
}

func (r *rLock) lease(key, value string) bool {
	if r.notSupportLua {
		v, err := r.rClient.Get(context.Background(), key).Result()
		if err != nil {
			return false
		}
		if v != value {
			return false
		}
		cmdRet, err := r.rClient.Expire(context.Background(), key, lockedLife).Result()
		if err != nil {
			return false
		}
		return cmdRet
	}
	cmd := r.rClient.Eval(context.Background(), luaExtendScript, []string{key}, value, lockedLife.Seconds())
	return cmd.Val().(int64) == 1
}

type state struct {
	watchChan chan dlock.WatchState
	key       string
	value     string
}

func (st *state) GetStateStream() <-chan dlock.WatchState {
	return st.watchChan
}
