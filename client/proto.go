package client

const (
	HeaderSize     int = 20
	RespHeaderSize     = 10
)

type CommandEnum uint8

func (ce CommandEnum) Int() int {
	return int(ce)
}

func (ce CommandEnum) Byte() byte {
	return byte(ce)
}

const (
	CommandSub         CommandEnum = 0
	CommandPub         CommandEnum = 1
	CommandCreateTopic CommandEnum = 2
	CommandDeleteTopic CommandEnum = 3

	CommandDelay     CommandEnum = 16
	CommandAlive     CommandEnum = 17
	CommandReplica   CommandEnum = 64
	CommandTopicInfo CommandEnum = 65
	CommandList      CommandEnum = 100
)

type BizError struct {
	message string
}

func (e *BizError) Error() string {
	return e.message
}

func IsBizErr(err error) bool {
	if err == nil {
		return false
	}
	_, ok := err.(*BizError)
	return ok
}

func NewBizError(msg string) error {
	return &BizError{
		message: msg,
	}
}
