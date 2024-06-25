package client

import (
	"encoding/binary"
	"errors"
)

var BuffNotEnoughErr = errors.New("buff data is not enough")

type Header struct {
	Key   string
	Value string
}

func (h *Header) ToBytes() []byte {
	var buf []byte
	buf = append(buf, VarString(h.Key)...)
	buf = append(buf, VarString(h.Value)...)
	return buf
}

type Message struct {
	headers   []*Header
	headerMap map[string]*Header
	payload   []byte
}

func NewMessage(payload []byte) *Message {
	return &Message{
		headerMap: map[string]*Header{},
		payload:   payload,
	}
}

func NewEmptyMessage() *Message {
	return &Message{
		headerMap: map[string]*Header{},
	}
}

func (m *Message) AddHeader(key string, value string) {
	v := m.headerMap[key]
	if v != nil {
		v.Value = value
		return
	}
	v = &Header{
		Key:   key,
		Value: value,
	}

	m.headers = append(m.headers, v)
	m.headerMap[key] = v
}

func (m *Message) ToBytes() []byte {
	buf := make([]byte, 8)
	binary.LittleEndian.PutUint32(buf[4:], uint32(len(m.headers)))
	for _, h := range m.headers {
		buf = append(buf, h.ToBytes()...)
	}
	buf = append(buf, m.payload...)
	binary.LittleEndian.PutUint32(buf[:4], uint32(len(buf)-8))
	return buf
}
func (m *Message) GetHeaderCount() int {
	return len(m.headers)
}

func (m *Message) GetHeaders() []*Header {
	return m.headers
}

func (m *Message) GetPayload() []byte {
	return m.payload
}

type SubMessage struct {
	*Message
	Ts     int64
	Id     int64
	FileId int64
	Pos    int64
}

func (sm *SubMessage) FromBytes(buf []byte) (int, error) {
	if len(buf) < 40 {
		return 0, BuffNotEnoughErr
	}
	sm.Message = NewEmptyMessage()
	sm.Ts = int64(binary.LittleEndian.Uint64(buf))
	sm.Id = int64(binary.LittleEndian.Uint64(buf[8:]))
	sm.FileId = int64(binary.LittleEndian.Uint64(buf[16:]))
	sm.Pos = int64(binary.LittleEndian.Uint64(buf[24:]))
	payloadSize := int(binary.LittleEndian.Uint32(buf[32:]))
	headerCount := int(binary.LittleEndian.Uint32(buf[36:]))
	msgBuf := buf[40:]

	headerSize := 0

	for ; headerCount > 0; headerCount-- {
		n, key, err := ParseVarString(msgBuf)
		if err != nil {
			return 0, err
		}
		headerSize += n
		msgBuf = msgBuf[n:]
		n, value, err := ParseVarString(msgBuf)
		if err != nil {
			return 0, err
		}
		headerSize += n
		msgBuf = msgBuf[n:]
		sm.AddHeader(key, value)
	}

	bodySize := payloadSize - headerSize
	sm.Message.payload = make([]byte, bodySize)

	if len(msgBuf) < bodySize {
		return 0, BuffNotEnoughErr
	}

	copy(sm.Message.payload, msgBuf[:bodySize])

	return 40 + payloadSize, nil
}
