package logger

import (
	"bytes"
	"fmt"
	"log"
	"runtime"
	"strconv"
)

type Logger interface {
	Debugf(template string, args ...interface{})
	Infof(template string, args ...interface{})
	Warnf(template string, args ...interface{})
	Errorf(template string, args ...interface{})
}

func GetGoroutineID() uint64 {
	b := make([]byte, 64)
	b = b[:runtime.Stack(b, false)]
	b = bytes.TrimPrefix(b, []byte("goroutine "))
	b = b[:bytes.IndexByte(b, ' ')]
	n, _ := strconv.ParseUint(string(b), 10, 64)
	return n
}

var LoggerInstance = &defaultLogger{}

func Debugf(template string, args ...interface{}) {
	LoggerInstance.Debugf(template, args...)
}

func Infof(template string, args ...interface{}) {
	LoggerInstance.Infof(template, args...)
}

func Warnf(template string, args ...interface{}) {
	LoggerInstance.Warnf(template, args...)
}

func Errorf(template string, args ...interface{}) {
	LoggerInstance.Errorf(template, args...)
}

type defaultLogger struct {
}

func (l *defaultLogger) Debugf(template string, args ...interface{}) {
	log.Printf(completeTemplate(template), args...)
}

func (l *defaultLogger) Infof(template string, args ...interface{}) {
	log.Printf(completeTemplate(template), args...)
}

func (l *defaultLogger) Warnf(template string, args ...interface{}) {
	log.Printf(completeTemplate(template), args...)
}
func (l *defaultLogger) Errorf(template string, args ...interface{}) {
	log.Printf(completeTemplate(template), args...)
}

func completeTemplate(template string) string {
	return fmt.Sprintf("gid=%d,%s\n", GetGoroutineID(), template)
}
