package client

import (
	"fmt"
	"github.com/rolandhe/smss/smss-client/dlock"
	"github.com/rolandhe/smss/smss-client/logger"
	"sync"
	"sync/atomic"
	"time"
)

type DLockSub interface {
	Sub(eventId int64, batchSize uint8, ackTimeout time.Duration, accept MessagesAccept) error
}

func NewDLockSub(topicName, who, host string, port int, timeout time.Duration, locker dlock.DLock, syncWatch bool) DLockSub {
	return &dLockedSub{
		clientCreateFunc: func() (*SubClient, error) {
			return NewSubClient(topicName, who, host, port, timeout)
		},
		locker:    locker,
		key:       fmt.Sprintf("sub_lock@%s@%s", topicName, who),
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
			logger.Infof("get lock,and start thread to subscribe")
			go func() {
				r.run()
				close(r.closeChan)
			}()
			continue
		}
		if state == dlock.LostLock {
			logger.Infof("loss lock,release resource")
			r.cleanResource()
			continue
		}
		logger.Infof("lock state %v", state)
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
				logger.Infof("subcribe closed,no client,return")
				return
			}
			logger.Infof("connect smss server err,sleep 5s:%v", err)
			time.Sleep(time.Second * 5)
			continue
		}
		if r.closedState.Load() {
			client.Close()
			logger.Infof("subcribe closed,release client,return")
			return
		}
		break
	}

	r.Lock()
	if r.closedState.Load() {
		client.Close()
		logger.Infof("subcribe closed,release client,return")
		return
	}
	r.runClient = client
	r.Unlock()

	logger.Infof("to subcribe,eventId=%d", r.eventId)
	err = client.Sub(r.eventId, r.batchSize, r.ackTimeout, func(messages []*SubMessage) AckEnum {
		last := messages[len(messages)-1]
		ret := r.accept(messages)
		r.eventId = last.EventId
		return ret
	})

	logger.Infof("sub error:%v", err)

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
		logger.Infof("subcribe end,client close")
		<-r.closeChan
	}
}
