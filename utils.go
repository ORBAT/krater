package krater

import (
	"fmt"
	"io"
	"io/ioutil"
	"log"

	"gopkg.in/Shopify/sarama.v1"
)

func sequentialIntGen() func() int {
	i := 0
	return func() int {
		i++
		return i
	}
}

// LogOutput is the writer used by krater's loggers
var LogOutput io.Writer = ioutil.Discard

// newLogger creates a new log.Logger using either "out" or, if it's nil, LogOutput for writing.
func newLogger(prefix string, out io.Writer) StdLogger {
	if out == nil {
		out = LogOutput
	}
	return log.New(out, fmt.Sprintf("[%s] ", prefix), log.Ldate|log.Lmicroseconds)
}

// LogTo makes krater and sarama loggers output to the given writer by replacing sarama.Logger and LogOutput. It returns a function that
// sets LogOutput and sarama.Logger to whatever values they had before the call to LogTo.
//
// As an example
//     defer LogTo(os.Stderr)()
// would start logging to stderr immediately and defer restoring the loggers.
func LogTo(w io.Writer) func() {
	oldLogger := sarama.Logger
	oldOutput := LogOutput
	LogOutput = w
	sarama.Logger = newLogger("Sarama", LogOutput)
	return func() {
		LogOutput = oldOutput
		sarama.Logger = oldLogger
	}
}

// StdLogger is the interface for log.Logger-compatible loggers
type StdLogger interface {
	Print(v ...interface{})
	Printf(format string, v ...interface{})
	Println(v ...interface{})
	Panic(v ...interface{})
	Panicf(format string, v ...interface{})
}

func withRecover(fn func()) {
	defer func() {
		handler := PanicHandler
		if handler != nil {
			if err := recover(); err != nil {
				handler(err)
			}
		}
	}()

	fn()
}

// PanicHandler is called for recovering from panics spawned internally to the library (and thus
// not recoverable by the caller's goroutine). Defaults to nil, which means panics are not recovered.
var PanicHandler func(interface{})
