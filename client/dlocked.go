package client

import (
	"fmt"
	"github.com/rolandhe/smss/smss-client/dlock"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

type DLockSub interface {
	Sub(eventId int64, batchSize uint8, ackTimeout time.Duration, accept MessagesAccept) error
}

func NewDLockSub(mqName, who, host string, port int, timeout time.Duration, locker dlock.DLock, syncWatch bool) DLockSub {
	return &dLockedSub{
		clientCreateFunc: func() (*SubClient, error) {
			return NewSubClient(mqName, who, host, port, timeout)
		},
		locker:    locker,
		key:       fmt.Sprintf("sub_lock@%s", who),
		syncWatch: syncWatch,
	}
}

type dLockedSub struct {
	clientCreateFunc func() (*SubClient, error)
	locker           dlock.DLock
	key              string
	syncWatch        bool
}

func (sub *dLockedSub) Sub(eventId int64, batchSize uint8, ackTimeout time.Duration, accept MessagesAccept) error {
	watcher, err := sub.locker.LockWatcher(sub.key)
	if err != nil {
		return err
	}

	r := &watchRunning{
		eventId:          eventId,
		batchSize:        batchSize,
		ackTimeout:       ackTimeout,
		accept:           accept,
		clientCreateFunc: sub.clientCreateFunc,
	}

	if sub.syncWatch {
		sub.doWatch(watcher, r)
	} else {
		go sub.doWatch(watcher, r)
	}

	return nil
}

func (sub *dLockedSub) doWatch(watchChan <-chan dlock.WatchState, r *watchRunning) {
	for {
		var state dlock.WatchState
		select {
		case state = <-watchChan:
		case <-time.After(time.Second * 75):
			continue
		}
		if state == dlock.Locked {
			log.Printf("get lock,and start thread to subscribe\n")
			go func() {
				r.run()
				close(r.closeChan)
			}()
			continue
		}
		if state == dlock.LostLock {
			log.Printf("loss lock,release resource\n")
			r.cleanResource()
			continue
		}
		log.Printf("lock state %v\n", state)
	}
}

type watchRunning struct {
	eventId          int64
	batchSize        uint8
	ackTimeout       time.Duration
	accept           MessagesAccept
	clientCreateFunc func() (*SubClient, error)

	sync.Mutex
	closedState atomic.Bool
	runClient   *SubClient

	closeChan chan struct{}
}

func (r *watchRunning) run() {
	r.reset()
	for {
		if r.closedState.Load() {
			break
		}
		r.runCore()
	}
}

func (r *watchRunning) reset() {
	r.closeChan = make(chan struct{})
	r.closedState.Store(false)
}

func (r *watchRunning) runCore() {
	var err error
	var client *SubClient
	for {
		client, err = r.clientCreateFunc()
		if err != nil {
			if r.closedState.Load() {
				log.Printf("subcribe closed,no client,return\n")
				return
			}
			log.Printf("connect smss server err,sleep 5s:%v\n", err)
			time.Sleep(time.Second * 5)
			continue
		}
		if r.closedState.Load() {
			client.Close()
			log.Printf("subcribe closed,release client,return\n")
			return
		}
		break
	}

	r.Lock()
	if r.closedState.Load() {
		client.Close()
		log.Printf("subcribe closed,release client,return\n")
		return
	}
	r.runClient = client
	r.Unlock()

	log.Printf("to subcribe,eventId=%d\n", r.eventId)
	err = client.Sub(r.eventId, r.batchSize, r.ackTimeout, func(messages []*SubMessage) AckEnum {
		last := messages[len(messages)-1]
		ret := r.accept(messages)
		r.eventId = last.EventId
		return ret
	})

	log.Printf("sub error:%v\n", err)

}

func (r *watchRunning) cleanResource() {
	r.closedState.Store(true)
	var client *SubClient
	r.Lock()
	client = r.runClient
	r.runClient = nil
	r.Unlock()

	if client != nil {
		client.Close()
		log.Printf("subcribe end,client close\n")
		<-r.closeChan
	}
}
