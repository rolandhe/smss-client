package client

import (
	"encoding/binary"
	"log"
	"time"
)

type AckEnum byte

func (a AckEnum) Value() byte {
	return byte(a)
}

const (
	Ack            AckEnum = 0
	ActWithEnd     AckEnum = 1
	ActWithTermite AckEnum = 2
)

type SubClient struct {
	*network
	mqName string
	who    string
}

type MessagesAccept func(messages []*SubMessage) AckEnum

func NewSubClient(mqName, who, host string, port int, timeout time.Duration) (*SubClient, error) {
	nw, err := newNetwork(host, port, timeout)
	if err != nil {
		return nil, err
	}
	return &SubClient{
		network: nw,
		mqName:  mqName,
		who:     who,
	}, nil
}

func (sc *SubClient) Sub(messageId int64, batchSize uint8, ackTimeout time.Duration, accept MessagesAccept) error {
	var err error
	defer func() {
		if err != nil && !IsBizErr(err) {
			sc.Close()
		}
	}()
	if err = sc.init(); err != nil {
		return err
	}
	buf := sc.packageSubCmd(messageId, batchSize, ackTimeout)
	if err = writeAll(sc.conn, buf, sc.ioTimeout); err != nil {
		return err
	}
	err = sc.waitMessage(accept)

	log.Printf("wait message:%v\n", err)
	return err
}

func (sc *SubClient) waitMessage(accept MessagesAccept) error {
	var respHeader SubRespHeader
	var err error
	for {
		if err = readAll(sc.conn, respHeader.buf[:], sc.ioTimeout); err != nil {
			if isTimeoutError(err) {
				log.Printf("wait message timeout\n")
				continue
			}
			return err
		}
		code := respHeader.GetCode()
		if code == ErrCode {
			msgLen := int(binary.LittleEndian.Uint16(respHeader.buf[2:]))
			return readErrCodeMsg(sc.conn, msgLen, sc.ioTimeout)
		}

		if code == AliveCode {
			log.Printf("sub is alive\n")
			continue
		}
		if code == SubEndCode {
			log.Printf("peer notify to end,maybe mq deleted,end sub\n")
			return nil
		}
		if code != OkCode {
			log.Printf("not support response code\n")
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

		ack := accept(messages)
		if ActWithTermite == ack {
			return nil
		}
		if err = sc.ack(ack); err != nil {
			return err
		}
		if ack == ActWithEnd {
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
	size := 20
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
	return int(srh.buf[2])
}

func (srh *SubRespHeader) GetPayloadSize() int {
	return int(binary.LittleEndian.Uint32(srh.buf[4:]))
}
