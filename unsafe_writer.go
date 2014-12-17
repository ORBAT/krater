package krater

import (
	"fmt"
	"io"
	"io/ioutil"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"

	"github.com/Shopify/sarama"
)

// UnsafeWriter is an io.Writer that writes messages to Kafka, ignoring (or logging; see UnsafeWriter#SetLogger
// and LogOutput) any error responses sent by the brokers.
// Parallel calls to Write / ReadFrom are safe.
//
// It's recommended to initialize the sarama.Producer passed to UnsafeWriter with AckSuccesses = false.
//
// Close() must be called when the writer is no longer needed.
type UnsafeWriter struct {
	kp        Producer
	id        string
	topic     string
	closed    int32         // nonzero if the writer has started closing. Must be accessed atomically
	stopCh    chan struct{} // used to signal event loop close
	log       StdLogger
	pendingWg sync.WaitGroup // WaitGroup for pending messages
	closeMut  sync.Mutex     // mutex for Close and CloseAll
}

var unswIdGen = sequentialIntGen()

func NewUnsafeWriter(topic string, kp Producer) *UnsafeWriter {
	id := "aw-" + strconv.Itoa(unswIdGen())
	log := NewLogger(fmt.Sprintf("UnsafeWr %s -> %s", id, topic), nil)
	stopCh := make(chan struct{})

	uw := &UnsafeWriter{kp: kp, id: id, topic: topic, log: log, stopCh: stopCh}

	evtLoop := func() {
		log.Println("Starting error listener")
		for {
			select {
			case <-stopCh:
				log.Println("Closing error listener")
				return
			case perr, ok := <-kp.Errors():
				if !ok {
					log.Println("Errors() channel closed?!")
					return
				}
				log.Println("Got error from Kafka:", perr)
			case _, ok := <-kp.Successes():
				if !ok {
					log.Println("Successes() channel closed?!")
					return
				}
			}
		}
	}

	go withRecover(evtLoop)

	return uw
}

// Write writes byte slices to Kafka without checking for error responses. n will always be len(p) and err will be nil.
// Trying to Write to a closed writer will return syscall.EINVAL. Thread-safe.
//
// Write might block if the Input() channel of the underlying sarama.Producer is full.
func (uw *UnsafeWriter) Write(p []byte) (n int, err error) {
	if uw.Closed() {
		return 0, syscall.EINVAL
	}

	uw.pendingWg.Add(1)
	defer uw.pendingWg.Done()

	n = len(p)

	uw.kp.Input() <- &sarama.MessageToSend{Topic: uw.topic, Key: nil, Value: sarama.ByteEncoder(p)}

	return
}

// ReadFrom reads all available bytes from r and writes them to Kafka without checking for broker error responses. The returned
// error will be either nil or anything returned when reading from r. The returned int64 will always be the total length of bytes read from r
// or 0 if reading from r returned an error. Trying to ReadFrom using a closed Writer will return syscall.EINVAL.
//
// Note that UnsafeWriter doesn't support "streaming", so r is read in full before it's sent.
//
// Implements io.ReaderFrom.
func (uw *UnsafeWriter) ReadFrom(r io.Reader) (int64, error) {
	if uw.Closed() {
		return 0, syscall.EINVAL
	}

	bs, err := ioutil.ReadAll(r)
	if err != nil {
		return 0, err
	}
	ni, _ := uw.Write(bs)
	return int64(ni), nil
}

// Closed returns true if the UnsafeWriter has been closed, false otherwise. Thread-safe.
func (uw *UnsafeWriter) Closed() bool {
	return atomic.LoadInt32(&uw.closed) != 0
}

// SetLogger sets the logger used by this UnsafeWriter. Not thread-safe.
func (uw *UnsafeWriter) SetLogger(l StdLogger) {
	uw.log = l
}

// Close closes the writer. If the writer has already been closed, Close will return syscall.EINVAL. Thread-safe.
func (uw *UnsafeWriter) Close() (err error) {
	uw.log.Println("Close() called")
	uw.closeMut.Lock()
	defer uw.closeMut.Unlock()

	uw.log.Println("Close() mutex acquired")

	if uw.Closed() {
		return syscall.EINVAL
	}

	atomic.StoreInt32(&uw.closed, 1)

	uw.pendingWg.Wait()
	close(uw.stopCh)

	return nil
}
