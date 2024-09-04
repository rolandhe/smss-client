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

func NewDLockSub(topicName, who, host string, port int, timeout time.Duration, locker dlock.SubLock) DLockSub {
	return &dLockedSub{
		clientCreateFunc: func() (*SubClient, error) {
			return NewSubClient(topicName, who, host, port, timeout)
		},
		locker: locker,
		key:    fmt.Sprintf("sub_lock@%s@%s", topicName, who),
	}
}

type dLockedSub struct {
	clientCreateFunc func() (*SubClient, error)
	locker           dlock.SubLock
	key              string
}

func (sub *dLockedSub) Sub(eventId int64, batchSize uint8, ackTimeout time.Duration, accept MessagesAccept) error {
	r := &watchRunning{
		eventId:          eventId,
		batchSize:        batchSize,
		ackTimeout:       ackTimeout,
		accept:           accept,
		clientCreateFunc: sub.clientCreateFunc,
	}
	err := sub.locker.LockWatcher(sub.key, func(state dlock.WatchState) {
		if state == dlock.Locked {
			logger.Infof("get lock,and start thread to subscribe")
			go func() {
				r.run()
				close(r.closeChan)
			}()
			return
		}
		if state == dlock.LostLock || state == dlock.LockerShutdown {
			logger.Infof("release resource,state=%v", state)
			r.cleanResource()
			return
		}
		logger.Infof("lock state %v", state)
	})
	if err != nil {
		return err
	}
	return nil
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
