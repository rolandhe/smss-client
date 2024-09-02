package logger

import "log"

type Logger interface {
	Debugf(template string, args ...interface{})
	Infof(template string, args ...interface{})
	Warnf(template string, args ...interface{})
	Errorf(template string, args ...interface{})
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
	log.Printf(template+"\n", args...)
}

func (l *defaultLogger) Infof(template string, args ...interface{}) {
	log.Printf(template+"\n", args...)
}

func (l *defaultLogger) Warnf(template string, args ...interface{}) {
	log.Printf(template+"\n", args...)
}
func (l *defaultLogger) Errorf(template string, args ...interface{}) {
	log.Printf(template+"\n", args...)
}
