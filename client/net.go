package client

import (
	"fmt"
	"net"
	"time"
)

const (
	OkCode     = 200
	AliveCode  = 201
	SubEndCode = 255
	ErrCode    = 400
)

func writeAll(conn net.Conn, buf []byte, timeout time.Duration) error {
	for {
		l := len(buf)
		conn.SetWriteDeadline(time.Now().Add(timeout))
		n, err := conn.Write(buf)
		if err != nil {
			return err
		}
		if n == l {
			return nil
		}
		buf = buf[n:]
	}
}

func readAll(conn net.Conn, buff []byte, timeout time.Duration) error {
	rb := buff[:]
	all := 0
	for {
		conn.SetReadDeadline(time.Now().Add(timeout))
		n, err := conn.Read(rb)
		if err != nil {
			return err
		}
		all += n
		if all == len(buff) {
			break
		}
		rb = rb[n:]
	}
	return nil
}

type network struct {
	host           string
	port           int
	conn           net.Conn
	connectTimeout time.Duration
	ioTimeout      time.Duration
}

func (nw *network) init() error {
	if nw.conn != nil {
		return nil
	}
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("%s:%d", nw.host, nw.port), nw.connectTimeout)
	if err != nil {
		return err
	}
	nw.conn = conn
	return nil
}
func (nw *network) Close() {
	if nw.conn == nil {
		return
	}
	nw.conn.Close()
	nw.conn = nil
}

func newNetwork(host string, port int, timeout time.Duration) (*network, error) {
	nw := &network{
		host:           host,
		port:           port,
		connectTimeout: timeout,
		ioTimeout:      timeout,
	}
	if err := nw.init(); err != nil {
		return nil, err
	}
	return nw, nil
}

func readErrCodeMsg(conn net.Conn, errMsgLen int, ioTimeout time.Duration) error {

	if errMsgLen == 0 {
		return NewBizError("unknown err")
	}

	eMsgBuf := make([]byte, errMsgLen)

	if err := readAll(conn, eMsgBuf, ioTimeout); err != nil {
		return err
	}

	return NewBizError(string(eMsgBuf))
}

func isTimeoutError(err error) bool {
	// 检查是否为 net.Error 类型并且是否为超时错误
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		return true
	}
	return false
}
func readMQListBody(conn net.Conn, bodyLen int, ioTimeout time.Duration) (string, error) {
	if bodyLen == 0 {
		return "", nil
	}
	eMsgBuf := make([]byte, bodyLen)

	if err := readAll(conn, eMsgBuf, ioTimeout); err != nil {
		return "", err
	}

	return string(eMsgBuf), nil
}
