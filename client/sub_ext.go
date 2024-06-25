package client

func parseMessages(content []byte, msgCount int) ([]*SubMessage, error) {
	ret := make([]*SubMessage, msgCount)

	buf := content

	for i := 0; i < msgCount; i++ {
		msg := &SubMessage{}
		n, err := msg.FromBytes(buf)
		if err != nil {
			return nil, err
		}
		ret[i] = msg
		buf = buf[n:]
	}
	return ret, nil
}
