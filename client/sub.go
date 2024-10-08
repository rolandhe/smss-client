package client

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/rolandhe/smss-client/logger"
	"sync/atomic"
	"time"
)

type AckEnum byte

var TermiteError = errors.New("TermiteError")

func (a AckEnum) Value() byte {
	return byte(a)
}

const (
	Ack               AckEnum = 0
	AckWithEnd        AckEnum = 1
	TermiteWithoutAck AckEnum = 2
)

type SubClient struct {
	*network
	mqName           string
	who              string
	maxNoDataTimeout int64
	termiteState     atomic.Bool

	key string
}

type MessagesAccept func(messages []*SubMessage) AckEnum

func NewSubClient(topicName, who, host string, port int, timeout time.Duration) (*SubClient, error) {
	nw, err := newNetwork(host, port, timeout, true)
	if err != nil {
		return nil, err
	}
	return &SubClient{
		network:          nw,
		mqName:           topicName,
		who:              who,
		maxNoDataTimeout: 35 * 1000,
		key:              fmt.Sprintf("sid=%s.%s", topicName, who),
	}, nil
}

func (sc *SubClient) Sub(eventId int64, batchSize uint8, ackTimeout time.Duration, accept MessagesAccept, afterAck func(lastEventId int64, ack AckEnum, err error)) error {
	var err error
	buf := sc.packageSubCmd(eventId, batchSize, ackTimeout)
	if err = writeAll(sc.conn, buf, sc.ioTimeout); err != nil {
		return err
	}

	if afterAck == nil {
		afterAck = func(lastEventId int64, ack AckEnum, err error) {}
	}
	err = sc.waitMessage(accept, afterAck)

	logger.Infof("%s,wait message:%v", sc.key, err)
	return err
}

func (sc *SubClient) Termite() {
	sc.termiteState.Store(true)
	sc.Close()
}

func (sc *SubClient) waitMessage(accept MessagesAccept, afterAck func(lastEventId int64, ack AckEnum, err error)) error {
	var respHeader SubRespHeader
	var err error
	lastTime := time.Now().UnixMilli()
	for {
		if err = readAll(sc.conn, respHeader.buf[:], sc.ioTimeout); err != nil {
			if sc.termiteState.Load() {
				logger.Infof("%s,readAll met err, but termite:%v", sc.key, err)
				return TermiteError
			}
			if isTimeoutError(err) {
				if time.Now().UnixMilli()-lastTime > sc.maxNoDataTimeout {
					logger.Infof("%s,wait message timeout too long,maybe server dead", sc.key)
					return err
				}
				logger.Infof("%s,wait message timeout,continue...", sc.key)
				continue
			}
			return err
		}
		code := respHeader.GetCode()
		if code == ErrCode {
			msgLen := int(binary.LittleEndian.Uint16(respHeader.buf[2:]))
			return readErrCodeMsg(sc.conn, msgLen, sc.ioTimeout)
		}

		if code == SubEndCode {
			logger.Infof("%s,peer notify to end,maybe mq deleted,end sub", sc.key)
			return nil
		}

		lastTime = time.Now().UnixMilli()
		if code == AliveCode {
			logger.Infof("%s,sub is alive", sc.key)
			continue
		}

		if code != OkCode {
			logger.Infof("%s,not support response code", sc.key)
			return nil
		}

		msgCount := respHeader.GetMessageCount()
		content := make([]byte, respHeader.GetPayloadSize())
		if err = readAll(sc.conn, content, sc.ioTimeout); err != nil {
			return err
		}
		var messages []*SubMessage
		if messages, err = parseMessages(content, msgCount); err != nil {
			return err
		}
		lastEventId := messages[len(messages)-1].EventId
		ack := accept(messages)
		if TermiteWithoutAck == ack {
			afterAck(lastEventId, ack, nil)
			return nil
		}
		if err = sc.ack(ack); err != nil {
			afterAck(lastEventId, ack, err)
			return err
		}
		if ack == AckWithEnd {
			afterAck(lastEventId, ack, nil)
			return nil
		}
	}
}

func (sc *SubClient) ack(ack AckEnum) error {
	buf := make([]byte, 2)
	binary.LittleEndian.PutUint16(buf, uint16(ack))
	return writeAll(sc.conn, buf, sc.ioTimeout)
}

func (sc *SubClient) calPackageSize() int {
	size := HeaderSize
	size += len(sc.mqName)
	size += 4
	size += len(sc.who)
	size += 16
	return size
}

func (sc *SubClient) packageSubCmd(messageId int64, batchSize uint8, ackTimeout time.Duration) []byte {
	buf := make([]byte, sc.calPackageSize())
	buf[0] = CommandSub.Byte()
	nameLen := len(sc.mqName)
	binary.LittleEndian.PutUint16(buf[1:], uint16(nameLen))
	buf[3] = batchSize
	buf[4] = 1

	next := buf[HeaderSize:]
	copy(next, sc.mqName)

	next = next[nameLen:]
	binary.LittleEndian.PutUint64(next, uint64(messageId))
	next = next[8:]
	binary.LittleEndian.PutUint64(next, uint64(ackTimeout))
	next = next[8:]
	lWho := len(sc.who)
	binary.LittleEndian.PutUint32(next, uint32(lWho))
	next = next[4:]
	copy(next, sc.who)

	return buf
}

type SubRespHeader struct {
	buf [RespHeaderSize]byte
}

func (srh *SubRespHeader) GetCode() int {
	return int(binary.LittleEndian.Uint16(srh.buf[:2]))
}

func (srh *SubRespHeader) GetMessageCount() int {
	return int(srh.buf[2] & 0xFF)
}

func (srh *SubRespHeader) GetPayloadSize() int {
	return int(binary.LittleEndian.Uint32(srh.buf[4:]))
}
