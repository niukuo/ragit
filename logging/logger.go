package logging

import (
	"fmt"
	"log"
	"os"
)

func newLogger(name string) Logger {
	l := &logger{
		logger: log.New(os.Stderr, name+" ", log.Ldate|log.Ltime|log.Lmicroseconds|log.Lshortfile),
	}
	return l
}

type logger struct {
	logger *log.Logger
}

func (l *logger) output(level string, v ...interface{}) {
	s := level + " "
	s += fmt.Sprint(v...)
	l.logger.Output(3, s)
}

func (l *logger) outputf(level string, format string, v ...interface{}) {
	s := level + " "
	s += fmt.Sprintf(format, v...)
	l.logger.Output(3, s)
}

func (l *logger) Debug(v ...interface{}) {
	l.output("DEBUG", v...)
}

func (l *logger) Debugf(format string, v ...interface{}) {
	l.outputf("DEBUG", format, v...)
}

func (l *logger) Info(v ...interface{}) {
	l.output("INFO", v...)
}

func (l *logger) Infof(format string, v ...interface{}) {
	l.outputf("INFO", format, v...)
}

func (l *logger) Warning(v ...interface{}) {
	l.output("WARNING", v...)
}

func (l *logger) Warningf(format string, v ...interface{}) {
	l.outputf("WARNING", format, v...)
}

func (l *logger) Error(v ...interface{}) {
	l.output("ERROR", v...)
}

func (l *logger) Errorf(format string, v ...interface{}) {
	l.outputf("ERROR", format, v...)
}
