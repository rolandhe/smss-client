package pool

import (
	"errors"
	"fmt"
	"github.com/rolandhe/smss/smss-client/logger"
	"sync"
	"sync/atomic"
	"time"
)

var TimeoutError = errors.New("timeout")
var ShutdownError = errors.New("shutdown")

func NewPool[T any](config *Config, factory ObjectFactory[T]) ObjPool[T] {
	var shutControl *shutdownControl
	if config.BackCheck {
		shutControl = &shutdownControl{
			shutdownChan: make(chan struct{}),
			waitChan:     make(chan struct{}),
		}
	} else {
		shutControl = &shutdownControl{}
	}

	po := &pool[T]{
		config: config,
		lilo: &fixedLilo[T]{
			max:     config.MaxSize,
			objs:    make([]*wrapObj[T], config.MaxSize),
			factory: factory,
		},

		shutdownCtrl: shutControl,
		sema:         newSema(config.MaxSize),
	}

	if config.BackCheck {
		go monitor(po)
	}

	return po
}

type pool[T any] struct {
	config *Config

	lilo *fixedLilo[T]

	shutdownCtrl *shutdownControl

	sema *semaWithCounter
}

func (p *pool[T]) Borrow() (*T, error) {
	if p.shutdownCtrl.showdownState.Load() {
		return nil, ShutdownError
	}
	ok, _ := p.sema.acquire(p.config.WaitTimeout)
	if !ok {
		logger.Infof("Borrow to get sema timeout")
		return nil, TimeoutError
	}
	if o := p.quickBorrow(); o != nil {
		return o, nil
	}

	created, err := p.lilo.createObj(p.config.MaxLifetime)
	if err != nil {
		p.sema.release()
		logger.Infof("Borrow create object err:%v", err)
		return nil, err
	}
	if p.config.LogDebug {
		logger.Infof("Borrowed new object,%v", created)
	}
	return created, nil
}

func (p *pool[T]) ShutDown() {
	p.shutdownCtrl.showdownState.Store(true)
	if p.config.BackCheck {
		close(p.shutdownCtrl.shutdownChan)
		<-p.shutdownCtrl.waitChan
	}

	list := p.lilo.dry()
	for _, ins := range list {
		p.lilo.destroyObj(ins)
	}
	logger.Infof("ShutDown,destroy objs:%d", len(list))
}

func (p *pool[T]) Return(ins *T, bad bool) error {
	wo, ok := p.lilo.exists(ins)
	if !ok {
		logger.Infof("return unknown object")
		return errors.New("no such borrowed object")
	}
	defer p.sema.release()

	wrap := wo.(*wrapObj[T])
	if bad || wrap.isExpired(time.Now().UnixMilli()) {
		logger.Infof("Return invalid obj, bad? %v, obj is:%v", bad, wrap)
		p.lilo.destroyObj(ins)
		return nil
	}
	if p.shutdownCtrl.showdownState.Load() {
		logger.Infof("Return valid obj, but pool shut down, destroy, obj is:%v", wrap)
		p.lilo.destroyObj(ins)
		return nil
	}
	if p.config.TestOnReturn {
		if err := p.lilo.valid(ins); err != nil {
			logger.Infof("Return invalid obj, ping failed, obj is:%v,err:%v", wrap, err)
			p.lilo.destroyObj(ins)
			return err
		}
	}
	addRet := p.lilo.add(wrap, func() bool {
		return p.shutdownCtrl.showdownState.Load()
	})

	if !addRet {
		logger.Infof("Return valid obj, but pool shut down, destroy, obj is:%v", wrap)
		p.lilo.destroyObj(ins)
		return nil
	}
	if p.config.LogDebug {
		logger.Infof("Return object add=%v,%v", addRet, wrap)
	}

	return nil
}

func (p *pool[T]) quickBorrow() *T {
	o := p.lilo.get()
	if o == nil {
		return nil
	}
	if p.config.TestOnBorrow {
		if err := p.lilo.valid(o.obj); err != nil {
			logger.Infof("Borrow pooled object,but Valid failed,to create")
			p.lilo.destroyObj(o.obj)
			return nil
		}
	}
	if p.config.LogDebug {
		logger.Infof("Borrowed pooled object,%v", o)
	}
	return o.obj
}

func (p *pool[T]) checkInvalidObjs() {
	got, _ := p.sema.quickAcquire()
	if !got {
		logger.Infof("checkInvalidObjs get token fail")
		return
	}
	defer p.sema.release()
	o, till := p.lilo.conditionFirst()
	count := 0
	for {
		if o == nil {
			logger.Infof("checkInvalidObjs finish,checked:%d", count)
			return
		}
		count++
		if o.isExpired(time.Now().UnixMilli()) {
			logger.Infof("checkInvalidObjs,pooled obj expired,%v", o)
			p.lilo.destroyObj(o.obj)
			o = p.lilo.conditionGet(till)
			continue
		}

		if !p.config.TestOnCheck {
			p.lilo.add(o, nil)
			if p.config.LogDebug {
				logger.Infof("checkInvalidObjs, not need to valid, restore", o)
			}
			o = p.lilo.conditionGet(till)
			continue
		}
		if err := p.lilo.valid(o.obj); err != nil {
			logger.Infof("checkInvalidObjs, pooled obj %v ping err:%v", o, err)
			p.lilo.destroyObj(o.obj)
		} else {
			p.lilo.add(o, nil)
			if p.config.LogDebug {
				logger.Infof("checkInvalidObjs, valid ok, restore:%v", o)
			}
		}
		o = p.lilo.conditionGet(till)
	}
}

func (p *pool[T]) keepMinObjs() {
	count := 0
	for p.keepOneObject() {
		count++
	}
	logger.Infof("keepMinObjs new pooled obj count:%d", count)
}

func (p *pool[T]) keepOneObject() bool {
	ok, c := p.sema.quickAcquire()
	if !ok {
		logger.Infof("keepOneObject get sema failed")
		return false
	}
	defer p.sema.release()
	if c-1 >= int64(p.config.MinSize) {
		logger.Infof("keepOneObject pooled obj count is enough")
		return false
	}
	size := p.lilo.size()
	if p.config.MinSize <= size+int(c-1) {
		logger.Infof("keepOneObject pooled obj count is enough,no used in lilo:%d", size)
		return false
	}
	o, err := p.lilo.createObj(p.config.MaxLifetime)
	if err != nil {
		logger.Infof("keepOneObject met err:%v", err)
		return false
	}

	if !p.lilo.safeAdd(o, func(size int64) bool {
		return p.sema.getCount()-1+size < int64(p.config.MinSize)
	}) {
		p.lilo.destroyObj(o)
		logger.Infof("keepOneObject safe add failed,because count is enough")
		return false
	}

	if p.config.LogDebug {
		logger.Infof("keepOneObject, create new object:%v", o)
	}

	return true
}

func monitor[T any](p *pool[T]) {
	checkIntervalConf := p.config.CheckInterval
	firstTime := p.config.FirstCheck
	if firstTime == 0 {
		firstTime = time.Second
	}
	var checkInterval time.Duration
	firstRun := true
	for {
		if firstRun {
			checkInterval = firstTime
			firstRun = false
		} else {
			checkInterval = checkIntervalConf
		}
		select {
		case <-p.shutdownCtrl.shutdownChan:
			close(p.shutdownCtrl.waitChan)
			return
		case <-time.After(checkInterval):
		}

		p.checkInvalidObjs()
		p.keepMinObjs()
	}
}

type fixedLilo[T any] struct {
	sync.Mutex
	objs  []*wrapObj[T]
	max   int
	start int64
	end   int64

	returnObjs sync.Map
	factory    ObjectFactory[T]
}

func (fll *fixedLilo[T]) get() *wrapObj[T] {
	fll.Lock()
	defer fll.Unlock()
	size := int(fll.end - fll.start)
	if size == 0 {
		return nil
	}
	index := fll.start % int64(fll.max)
	ret := fll.objs[index]
	fll.start++
	return ret
}

func (fll *fixedLilo[T]) valid(ins *T) error {
	return fll.factory.Valid(ins)
}

func (fll *fixedLilo[T]) add(ins *wrapObj[T], shutdownFunc func() bool) bool {
	fll.Lock()
	defer fll.Unlock()
	if shutdownFunc != nil && shutdownFunc() {
		return false
	}
	index := fll.end % int64(fll.max)
	fll.objs[index] = ins
	fll.end++
	return true
}

func (fll *fixedLilo[T]) safeAdd(ins *T, checker func(size int64) bool) bool {
	wo, _ := fll.exists(ins)
	wrap := wo.(*wrapObj[T])
	fll.Lock()
	defer fll.Unlock()
	size := fll.end - fll.start
	if !checker(size) {
		return false
	}
	index := fll.end % int64(fll.max)
	fll.objs[index] = wrap
	fll.end++
	return true
}

func (fll *fixedLilo[T]) exists(ins *T) (any, bool) {
	return fll.returnObjs.Load(ins)
}

func (fll *fixedLilo[T]) size() int {
	fll.Lock()
	defer fll.Unlock()
	size := int(fll.end - fll.start)
	return size
}

func (fll *fixedLilo[T]) conditionGet(tillEnd int64) *wrapObj[T] {
	fll.Lock()
	defer fll.Unlock()
	if fll.start >= tillEnd {
		return nil
	}
	index := fll.start % int64(fll.max)
	ret := fll.objs[index]
	fll.start++
	return ret
}

func (fll *fixedLilo[T]) conditionFirst() (*wrapObj[T], int64) {
	fll.Lock()
	defer fll.Unlock()
	size := int(fll.end - fll.start)
	if size == 0 {
		return nil, 0
	}
	index := fll.start % int64(fll.max)
	ret := fll.objs[index]
	fll.start++
	return ret, fll.end
}

func (fll *fixedLilo[T]) createObj(maxLifetime int) (*T, error) {
	created, err := fll.factory.Create()
	if err != nil {
		return nil, err
	}
	wrap := &wrapObj[T]{
		maxLife: calMaxLife(maxLifetime),
		obj:     created,
	}
	fll.returnObjs.Store(created, wrap)
	return created, nil
}

func (fll *fixedLilo[T]) destroyObj(ins *T) {
	fll.returnObjs.Delete(ins)
	fll.factory.Destroy(ins)
}

func (fll *fixedLilo[T]) dry() []*T {
	fll.Lock()
	defer fll.Unlock()
	var ret []*T
	for fll.start < fll.end {
		index := fll.start % int64(fll.max)
		fll.start++
		ret = append(ret, fll.objs[index].obj)
	}
	return ret
}

type wrapObj[T any] struct {
	maxLife int64
	obj     *T
}

func (wo *wrapObj[T]) isExpired(now int64) bool {
	if wo.maxLife == 0 {
		return false
	}
	return now >= wo.maxLife
}

func (wo *wrapObj[T]) String() string {
	return fmt.Sprintf("maxLife:%d,obj:%v", wo.maxLife, wo.obj)
}

func calMaxLife(d int) int64 {
	if d == 0 {
		return 0
	}
	return time.Now().UnixMilli() + int64(d)
}

type shutdownControl struct {
	shutdownChan  chan struct{}
	waitChan      chan struct{}
	showdownState atomic.Bool
}

type semaWithCounter struct {
	ch      chan struct{}
	counter atomic.Int64
}

func newSema(max int) *semaWithCounter {
	return &semaWithCounter{
		ch: make(chan struct{}, max),
	}
}

func (sc *semaWithCounter) acquire(d time.Duration) (bool, int64) {
	if d == 0 {
		return sc.quickAcquire()
	}
	if d < 0 {
		sc.ch <- struct{}{}
		c := sc.counter.Add(1)
		return true, c
	}
	select {
	case sc.ch <- struct{}{}:
		c := sc.counter.Add(1)
		return true, c
	case <-time.After(d):
		return false, 0
	}
}

func (sc *semaWithCounter) quickAcquire() (bool, int64) {
	select {
	case sc.ch <- struct{}{}:
		c := sc.counter.Add(1)
		return true, c
	default:
		return false, 0
	}
}

func (sc *semaWithCounter) getCount() int64 {
	return sc.counter.Load()
}

func (sc *semaWithCounter) release() int64 {
	c := sc.counter.Add(-1)
	<-sc.ch
	return c
}
