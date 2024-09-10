package client

import (
	"github.com/rolandhe/smss-client/pool"
	"time"
)

type pubClientPoolAlias = pool.ObjPool[PubClient]

type PooledPubClient struct {
	*PubClient
	pubPool pubClientPoolAlias
}

func (ppc *PooledPubClient) Close() {
	ppc.pubPool.Return(ppc.PubClient, ppc.fetal)
}

type PubClientPool struct {
	internalPool pubClientPoolAlias
}

func (ppool *PubClientPool) Borrow() (*PooledPubClient, error) {
	o, err := ppool.internalPool.Borrow()
	if err != nil {
		return nil, err
	}
	return &PooledPubClient{o, ppool.internalPool}, nil
}

func (ppool *PubClientPool) Return(ins *PooledPubClient) error {
	return ppool.internalPool.Return(ins.PubClient, ins.fetal)
}

func (ppool *PubClientPool) ShutDown() {
	ppool.internalPool.ShutDown()
}

func NewPubClientPool(config *pool.Config, host string, port int, connectTimeout time.Duration) *PubClientPool {
	factory := &pubClientFactory{
		host:           host,
		port:           port,
		connectTimeout: connectTimeout,
	}
	internal := pool.NewPool[PubClient](config, factory)
	return &PubClientPool{
		internalPool: internal,
	}
}

type pubClientFactory struct {
	host           string
	port           int
	connectTimeout time.Duration
}

func (factory *pubClientFactory) Create() (*PubClient, error) {
	return NewPubClient(factory.host, factory.port, factory.connectTimeout)
}

func (factory *pubClientFactory) Valid(pc *PubClient) error {
	return pc.Alive("")
}

func (factory *pubClientFactory) Destroy(pc *PubClient) {
	pc.Close()
}
