package client

import (
	"encoding/binary"
	"log"
	"strconv"
	"strings"
	"time"
)

type ReplicaClient struct {
	*network
}

func NewReplicaClient(host string, port int, timeout time.Duration) (*ReplicaClient, error) {
	nw, err := newNetwork(host, port, timeout)
	if err != nil {
		return nil, err
	}
	return &ReplicaClient{
		network: nw,
	}, nil
}

func (rc *ReplicaClient) Replica(seqId int64) error {
	var err error
	defer func() {
		if err != nil && !IsBizErr(err) {
			rc.Close()
		}
	}()
	if err = rc.init(); err != nil {
		return err
	}
	buf := make([]byte, 28)
	buf[0] = byte(CommandReplica)
	binary.LittleEndian.PutUint64(buf[HeaderSize:], uint64(seqId))
	if err = writeAll(rc.conn, buf, rc.ioTimeout); err != nil {
		return err
	}
	err = rc.waitMessage()

	log.Printf("wait message:%v\n", err)
	return err
}

func (rc *ReplicaClient) waitMessage() error {
	var respHeader SubRespHeader
	var err error
	lenBuf := make([]byte, 4)
	for {
		if err = readAll(rc.conn, respHeader.buf[:], rc.ioTimeout); err != nil {
			if isTimeoutError(err) {
				log.Printf("wait message timeout\n")
				continue
			}
			return err
		}
		code := respHeader.GetCode()
		if code == ErrCode {
			msgLen := int(binary.LittleEndian.Uint16(respHeader.buf[2:]))
			return readErrCodeMsg(rc.conn, msgLen, rc.ioTimeout)
		}

		if code == AliveCode {
			log.Printf("sub is alive\n")
			continue
		}
		if code != OkCode {
			log.Printf("not support response code\n")
			return nil
		}

		if err = readAll(rc.conn, lenBuf, rc.ioTimeout); err != nil {
			return err
		}
		payLen := int(binary.LittleEndian.Uint32(lenBuf))
		payload := make([]byte, payLen)
		if err = readAll(rc.conn, payload, rc.ioTimeout); err != nil {
			return err
		}

		cmdLen := binary.LittleEndian.Uint32(payload)
		cmdBuf := payload[4 : cmdLen+4]
		body := payload[cmdLen+4:]

		cmdLine := CmdDecoder(cmdBuf)

		log.Printf("replica:seqId=%d, cmd=%d, l:%d , l1:%d\n", cmdLine.MessageSeqId, cmdLine.Command, cmdLine.PayloadLen, len(body))

		if err = rc.ack(Ack); err != nil {
			return err
		}
	}
}

func (rc *ReplicaClient) ack(ack AckEnum) error {
	buf := make([]byte, 2)
	binary.LittleEndian.PutUint16(buf, uint16(ack))
	return writeAll(rc.conn, buf, rc.ioTimeout)
}

type DecodedRawMessage struct {
	Src       uint8
	WriteTime int64
	Command   CommandEnum
	MqName    string
	// 服务端收到pub信息时的时间戳
	Timestamp int64

	MessageSeqId int64

	TraceId string

	Body       any
	PayloadLen int
}

func CmdDecoder(buf []byte) *DecodedRawMessage {
	cmdLine := string(buf[:len(buf)-1])
	items := strings.Split(cmdLine, "\t")
	var msg DecodedRawMessage

	msg.WriteTime, _ = strconv.ParseInt(items[0], 10, 64)

	v, _ := strconv.Atoi(items[1])
	msg.Command = CommandEnum(v)

	id, _ := strconv.Atoi(items[2])
	msg.MessageSeqId = int64(id)

	msg.Timestamp, _ = strconv.ParseInt(items[3], 10, 64)

	msg.MqName = items[4]
	msg.PayloadLen, _ = strconv.Atoi(items[5])

	return &msg
}
