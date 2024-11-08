package client

import (
	"fmt"
	"github.com/rolandhe/smss-client/dlock"
	"github.com/rolandhe/smss-client/logger"
	"sync"
	"sync/atomic"
	"time"
)

// DLockSub 带分布式锁的topic订阅者
type DLockSub interface {
	// Sub 订阅消息
	// eventId 已经消费完的消息的id，下一个消息将被推送
	// batchSize 支持批量消息订阅，batchSize支持批的大学，不要超过255
	// ackTimeout 告诉smss server 消息的最大处理时间，超过这个时间smss server如果还没有收到ack，smss server就认为socket已经断掉
	// accept 处理收到消息的回调函数
	// ack 执行完成后回调
	Sub(eventId int64, batchSize uint8, ackTimeout time.Duration, accept MessagesAccept, afterAck func(lastEventId int64, ack AckEnum, err error)) error
}

// NewDLockSub 创建支持分布式锁的订阅客户端, 分布式锁保证多个实例只有一个实例能够订阅消息, 并且当获取锁的实例崩溃后可以协调另一个实例继续消费
// topicName topic name
// who 当前订阅者是谁
// host smss server host
// port smss server port
// timeout 网络超时，包括 connect timeout and soTimeout
// locker 分布式锁
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

func (sub *dLockedSub) Sub(eventId int64, batchSize uint8, ackTimeout time.Duration, accept MessagesAccept, afterAck func(lastEventId int64, ack AckEnum, err error)) error {
	r := &watchRunning{
		eventId:          eventId,
		batchSize:        batchSize,
		ackTimeout:       ackTimeout,
		accept:           accept,
		clientCreateFunc: sub.clientCreateFunc,
	}
	if afterAck == nil {
		afterAck = func(lastEventId int64, ack AckEnum, err error) {}
	}
	r.afterAck = afterAck
	gotLock := false
	err := sub.locker.LockWatcher(sub.key, func(state dlock.WatchState) {
		if state == dlock.Locked {
			pgid := logger.GetGoroutineID()
			logger.Infof("get lock,and start thread to subscribe")
			r.initControl()
			go func() {
				logger.Infof("sub thread to subscribe,parentGid=%d", pgid)
				r.run()
				close(r.control.closedChan)
			}()
			gotLock = true
			return
		}
		if state == dlock.LostLock || state == dlock.LockerShutdown {
			logger.Infof("release resource,state=%v", state)
			if gotLock {
				r.cleanResource()
				gotLock = false
			}
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
	afterAck         func(lastEventId int64, ack AckEnum, err error)
	clientCreateFunc func() (*SubClient, error)

	control struct {
		closedState  atomic.Bool
		closeStateCh chan struct{}
		sync.Mutex
		runClient *SubClient

		closedChan chan struct{}
	}
}

func (r *watchRunning) run() {
	if r.control.closedState.Load() {
		return
	}
	for {
		r.runCore()
		logger.Infof("runCore exit, try to wait...")

		waitCloseFunc := func() bool {
			timer := time.NewTimer(time.Second * 5)
			defer timer.Stop()
			select {
			case <-r.control.closeStateCh:
				logger.Infof("lock shutdown, exit watchRunning.run")
				return true
			case <-timer.C:
				logger.Infof("wait 5s after r.runCore, try to run core again")
				return false
			}
		}

		if waitCloseFunc() {
			return
		}
	}
}

func (r *watchRunning) initControl() {
	r.control.closedChan = make(chan struct{})
	r.control.closedState.Store(false)
	r.control.closeStateCh = make(chan struct{})
	r.setClient(nil)
}

func (r *watchRunning) setClient(client *SubClient) bool {
	r.control.Lock()
	defer r.control.Unlock()

	if client == nil {
		r.control.runClient = nil
		return true
	}
	if r.control.closedState.Load() {
		r.control.runClient = nil
		return false
	}
	r.control.runClient = client
	return true
}

func (r *watchRunning) runCore() {
	var err error
	var client *SubClient
	client, err = r.clientCreateFunc()
	if err != nil {
		logger.Infof("runCore to create client failed, err:%v", err)
		return
	}

	defer client.Close()
	if !r.setClient(client) {
		logger.Infof("locker shutdown,runCore to release client,and return")
		return
	}

	logger.Infof("to subcribe,eventId=%d", r.eventId)
	err = client.Sub(r.eventId, r.batchSize, r.ackTimeout, func(messages []*SubMessage) AckEnum {
		last := messages[len(messages)-1]
		ret := r.accept(messages)
		r.eventId = last.EventId
		return ret
	}, r.afterAck)

	logger.Infof("sub error:%v", err)
	r.setClient(nil)
}

func (r *watchRunning) cleanResource() {
	r.control.closedState.Store(true)
	close(r.control.closeStateCh)
	var client *SubClient

	r.control.Lock()
	client = r.control.runClient
	r.control.runClient = nil
	r.control.Unlock()

	if client != nil {
		client.Close()
		logger.Infof("subcribe end,client close")
	}
	<-r.control.closedChan

	r.control.closeStateCh = nil
	r.control.closedChan = nil
}
