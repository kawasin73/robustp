package logger

import (
	"fmt"
	"log"
)

var loglv = LvDebug

const (
	LvError = iota + 1
	LvInfo
	LvDebug
)

func SetLevel(lv int) {
	loglv = lv
}

func output(lv int, prefix string, v ...interface{}) {
	if lv <= loglv {
		_ = log.Output(3, prefix+fmt.Sprintln(v...))
	}
}

func outputf(lv int, prefix string, format string, v ...interface{}) {
	if lv <= loglv {
		_ = log.Output(3, prefix+fmt.Sprintf(format, v...))
	}
}

func Panic(v ...interface{}) {
	log.Panic(v...)
}

func Error(v ...interface{}) {
	output(LvError, "[error] ", v...)
}

func Info(v ...interface{}) {
	output(LvInfo, "[info ] ", v...)
}

func Debug(v ...interface{}) {
	output(LvDebug, "[debug] ", v...)
}

func Errorf(format string, v ...interface{}) {
	outputf(LvError, "[error] ", format, v...)
}

func Infof(format string, v ...interface{}) {
	outputf(LvInfo, "[info ] ", format, v...)
}

func Debugf(format string, v ...interface{}) {
	outputf(LvDebug, "[debug] ", format, v...)
}
