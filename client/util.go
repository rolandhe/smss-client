package client

import (
	"encoding/binary"
)

func VarString(s string) []byte {
	l := len(s)
	if l == 0 {
		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf, uint32(l))
		return buf
	}

	buf := make([]byte, l+4)
	binary.LittleEndian.PutUint32(buf, uint32(l))
	copy(buf[4:], s)
	return buf
}

func ParseVarString(buf []byte) (int, string, error) {
	if len(buf) < 4 {
		return 0, "", BuffNotEnoughErr
	}
	l := int(binary.LittleEndian.Uint32(buf))
	if l == 0 {
		return 4, "", nil
	}
	if len(buf) < 4+l {
		return 0, "", BuffNotEnoughErr
	}
	return 4 + l, string(buf[4 : 4+l]), nil
}
