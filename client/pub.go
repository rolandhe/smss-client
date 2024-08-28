package client

import (
	"encoding/binary"
	"errors"
	"fmt"
	"time"
)

type PubClient struct {
	*network
	fetal bool
}

func NewPubClient(host string, port int, timeout time.Duration) (*PubClient, error) {
	nw, err := newNetwork(host, port, timeout)
	if err != nil {
		return nil, err
	}
	if err = nw.init(); err != nil {
		return nil, err
	}
	return &PubClient{
		network: nw,
	}, nil
}

func (pc *PubClient) Publish(topicName string, msg *Message, traceId string) error {
	payload := msg.ToBytes()
	payLoadLen := len(payload)
	if payLoadLen <= 8 {
		return errors.New("empty message")
	}
	traceIdLen := len(traceId)
	buf := make([]byte, HeaderSize+len(topicName)+payLoadLen+traceIdLen)
	buf[0] = CommandPub.Byte()
	buf[19] = byte(traceIdLen)
	binary.LittleEndian.PutUint16(buf[1:], uint16(len(topicName)))
	binary.LittleEndian.PutUint32(buf[3:], uint32(payLoadLen))

	dst := buf[HeaderSize:]
	n := copy(dst, topicName)
	dst = dst[n:]
	if traceIdLen > 0 {
		n = copy(dst, traceId)
		dst = dst[n:]
	}
	copy(dst, payload)
	payload = nil

	if err := writeAll(pc.conn, buf, pc.ioTimeout); err != nil {
		pc.fetal = true
		return err
	}

	buf = nil

	return pc.readResult(0)
}

func (pc *PubClient) PublishDelay(topicName string, msg *Message, delayMils int64, traceId string) error {
	payload := msg.ToBytes()
	payLoadLen := len(payload)
	if payLoadLen <= 8 {
		return errors.New("empty message")
	}
	traceIdLen := len(traceId)
	buf := make([]byte, HeaderSize+len(topicName)+len(payload)+8+traceIdLen)
	buf[0] = CommandDelay.Byte()
	buf[19] = byte(traceIdLen)
	binary.LittleEndian.PutUint16(buf[1:], uint16(len(topicName)))
	binary.LittleEndian.PutUint32(buf[3:], uint32(payLoadLen))

	dst := buf[HeaderSize:]
	n := copy(dst, topicName)
	dst = dst[n:]
	if traceIdLen > 0 {
		n = copy(dst, traceId)
		dst = dst[n:]
	}

	binary.LittleEndian.PutUint64(dst, uint64(delayMils))

	copy(dst[8:], payload)
	payload = nil

	if err := writeAll(pc.conn, buf, pc.ioTimeout); err != nil {
		pc.fetal = true
		return err
	}

	buf = nil

	return pc.readResult(0)
}

func (pc *PubClient) CreateTopic(topicName string, life int64, traceId string) error {
	traceIdLen := len(traceId)
	hBuf := make([]byte, HeaderSize)
	hBuf[0] = CommandCreateTopic.Byte()
	hBuf[19] = byte(traceIdLen)
	binary.LittleEndian.PutUint16(hBuf[1:], uint16(len(topicName)))
	hBuf = append(hBuf, []byte(topicName)...)

	if traceIdLen > 0 {
		hBuf = append(hBuf, []byte(traceId)...)
	}

	pBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(pBuf, uint64(life))
	hBuf = append(hBuf, pBuf...)

	if err := writeAll(pc.conn, hBuf, pc.ioTimeout); err != nil {
		pc.fetal = true
		return err
	}

	return pc.readResult(0)
}

func (pc *PubClient) DeleteTopic(topicName string, traceId string) error {
	traceIdLen := len(traceId)
	hBuf := make([]byte, HeaderSize)
	hBuf[0] = CommandDeleteTopic.Byte()
	hBuf[19] = byte(traceIdLen)
	binary.LittleEndian.PutUint16(hBuf[1:], uint16(len(topicName)))
	hBuf = append(hBuf, []byte(topicName)...)
	if traceIdLen > 0 {
		hBuf = append(hBuf, []byte(traceId)...)
	}
	if err := writeAll(pc.conn, hBuf, pc.ioTimeout); err != nil {
		pc.fetal = true
		return err
	}

	return pc.readResult(0)
}

func (pc *PubClient) GetTopicInfo(topicName, traceId string) (string, error) {
	traceIdLen := len(traceId)
	hBuf := make([]byte, HeaderSize)
	hBuf[0] = CommandTopicInfo.Byte()
	hBuf[19] = byte(traceIdLen)
	binary.LittleEndian.PutUint16(hBuf[1:], uint16(len(topicName)))
	hBuf = append(hBuf, []byte(topicName)...)
	if traceIdLen > 0 {
		hBuf = append(hBuf, []byte(traceId)...)
	}
	if err := writeAll(pc.conn, hBuf, pc.ioTimeout); err != nil {
		pc.fetal = true
		return "", err
	}
	return pc.readMqListResult()
}

func (pc *PubClient) GetTopicList(traceId string) (string, error) {
	traceIdLen := len(traceId)
	hBuf := make([]byte, HeaderSize)
	hBuf[0] = CommandList.Byte()
	hBuf[19] = byte(traceIdLen)
	if traceIdLen > 0 {
		hBuf = append(hBuf, []byte(traceId)...)
	}
	if err := writeAll(pc.conn, hBuf, pc.ioTimeout); err != nil {
		pc.fetal = true
		return "", err
	}
	return pc.readMqListResult()
}

func (pc *PubClient) Alive(traceId string) error {
	traceIdLen := len(traceId)
	hBuf := make([]byte, HeaderSize)
	hBuf[0] = CommandAlive.Byte()
	hBuf[19] = byte(traceIdLen)
	if traceIdLen > 0 {
		hBuf = append(hBuf, []byte(traceId)...)
	}
	if err := writeAll(pc.conn, hBuf, pc.ioTimeout); err != nil {
		pc.fetal = true
		return err
	}
	return pc.readResult(0)
}

func (pc *PubClient) String() string {
	return fmt.Sprintf("PubClient(%s)", pc.conn.RemoteAddr())
}

func (pc *PubClient) readResult(timeout time.Duration) error {
	if timeout == 0 {
		timeout = pc.ioTimeout
	}
	buf := make([]byte, RespHeaderSize)
	if err := readAll(pc.conn, buf, timeout); err != nil {
		return err
	}

	code := binary.LittleEndian.Uint16(buf)
	if code == OkCode || code == AliveCode {
		return nil
	}
	if code != ErrCode {
		pc.fetal = true
		return errors.New("not support code")
	}
	errMsgLen := int(binary.LittleEndian.Uint16(buf[2:]))
	err := readErrCodeMsg(pc.conn, errMsgLen, pc.ioTimeout)

	if err != nil && !IsBizErr(err) {
		pc.fetal = true
	}

	return err
}

func (pc *PubClient) readMqListResult() (string, error) {
	buf := make([]byte, RespHeaderSize)
	if err := readAll(pc.conn, buf, pc.ioTimeout); err != nil {
		return "", err
	}

	code := binary.LittleEndian.Uint16(buf)
	if code == OkCode {
		bodyLen := int(binary.LittleEndian.Uint32(buf[2:]))
		return readMQListBody(pc.conn, bodyLen, pc.ioTimeout)
	}
	if code != ErrCode {
		pc.fetal = true
		return "", errors.New("not support code")
	}
	errMsgLen := int(binary.LittleEndian.Uint16(buf[2:]))
	err := readErrCodeMsg(pc.conn, errMsgLen, pc.ioTimeout)
	if err != nil && !IsBizErr(err) {
		pc.fetal = true
	}
	return "", err
}
