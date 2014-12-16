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

type work struct {
	resultCh chan error // channel to return result of write on
	*sarama.MessageToSend
}

// AckingWriter is an io.Writer that writes messages to Kafka. Parallel calls to Write() will cause messages to be queued by the producer, and
// each Write() call will block until a response is received.
//
// The sarama.Producer passed to AckingWriter must have AckSuccesses = true.
//
// Close() must be called when the writer is no longer needed.
type AckingWriter struct {
	kp          Producer
	id          string
	topic       string
	stopCh      chan struct{} // used to signal event loop close
	closed      int32         // nonzero if the writer has started closing. Must be accessed atomically
	log         StdLogger
	closeMut    sync.Mutex                           // mutex for Close
	errChForMsg map[*sarama.MessageToSend]chan error // return channel for "work message" responses
	pendingWg   sync.WaitGroup                       // WaitGroup for pending message responses
	workCh      chan work                            // channel for sending writes to event loop
}

var awIdGen = SequentialIntGen()

// NewAckingWriter returns an AckingWriter that uses kp to produce messages to Kafka topic 'topic', with a maximum of maxConcurrent concurrent writes.
//
// kp MUST have been initialized with AckSuccesses = true or Write will block indefinitely.
func NewAckingWriter(topic string, kp Producer, maxConcurrent int) *AckingWriter {
	id := "aw-" + strconv.Itoa(awIdGen())
	logger := NewLogger(fmt.Sprintf("AckWr %s -> %s", id, topic), nil)
	aw := &AckingWriter{
		kp:          kp,
		id:          id,
		topic:       topic,
		log:         logger,
		stopCh:      make(chan struct{}),
		errChForMsg: make(map[*sarama.MessageToSend]chan error),
		workCh:      make(chan work, maxConcurrent)}

	evtLoop := func() {
		errCh := aw.kp.Errors()
		succCh := aw.kp.Successes()

		aw.log.Println("Starting event loop")
		for {
			select {
			case <-aw.stopCh:
				aw.log.Println("Stopping event loop")
				return
			case work := <-aw.workCh:
				aw.errChForMsg[work.MessageToSend] = work.resultCh
				// this is done in a new goroutine to prevent blocking the event loop in case Input() is full.
				// This could probably be improved somehow.
				go func() {
					aw.kp.Input() <- work.MessageToSend
				}()
			case perr, ok := <-errCh:
				if !ok {
					aw.log.Println("Errors() channel closed?!")
					close(aw.stopCh)
					continue
				}
				aw.sendProdResponse(perr.Msg, perr.Err)
			case succ, ok := <-succCh:
				if !ok {
					aw.log.Println("Successes() channel closed?!")
					close(aw.stopCh)
					continue
				}
				aw.sendProdResponse(succ, nil)
			}
		}
	}

	go withRecover(evtLoop)

	return aw
}

func (aw *AckingWriter) sendProdResponse(msg *sarama.MessageToSend, perr error) {
	receiver, ok := aw.errChForMsg[msg]
	if !ok {
		aw.log.Panicf("Nobody wanted *MessageToSend %p (%#v)?", msg, msg)
	} else {
		receiver <- perr
		delete(aw.errChForMsg, msg)
	}
}

// TODO(orbat): Write timeouts?

// Write will queue p as a single message, blocking until a response is received. n will always be len(p) if the
// message was sent successfully, 0 otherwise. Trying to Write to a closed writer will return syscall.EINVAL. Thread-safe.
//
// Implements io.Writer.
func (aw *AckingWriter) Write(p []byte) (n int, err error) {
	if aw.Closed() {
		return 0, syscall.EINVAL
	}

	n = len(p)
	resCh := make(chan error, 1)
	wrk := work{MessageToSend: &sarama.MessageToSend{Topic: aw.topic, Key: nil, Value: sarama.ByteEncoder(p)}, resultCh: resCh}

	aw.pendingWg.Add(1)
	defer aw.pendingWg.Done()

	aw.workCh <- wrk

	select {
	case resp := <-resCh:
		close(resCh)
		if resp != nil {
			err = resp
			n = 0
		}
	}

	return
}

// ReadFrom reads all available bytes from r and writes them to Kafka in a single message. The returned int64 will always be
// the total length of bytes read from r or 0 if reading from r returned an error. Trying to Write to a closed writer will
// return syscall.EINVAL
//
// Note that AckingWriter doesn't support "streaming", so r is read in full before it's sent.
//
// Implements io.ReaderFrom.
func (aw *AckingWriter) ReadFrom(r io.Reader) (n int64, err error) {
	if aw.Closed() {
		return 0, syscall.EINVAL
	}

	p, err := ioutil.ReadAll(r)
	if err != nil {
		return 0, err
	}

	ni, err := aw.Write(p)
	return int64(ni), err
}

// Closed returns true if the AckingWriter has been closed, false otherwise. Thread-safe.
func (aw *AckingWriter) Closed() bool {
	return atomic.LoadInt32(&aw.closed) != 0
}

// SetLogger sets the logger used by this AckingWriter. Not thread-safe.
func (aw *AckingWriter) SetLogger(l StdLogger) {
	aw.log = l
}

// Close closes the writer. If the writer has already been closed, Close will return syscall.EINVAL. Thread-safe.
func (aw *AckingWriter) Close() error {
	aw.log.Println("Close() called")
	aw.closeMut.Lock()
	defer aw.closeMut.Unlock()

	aw.log.Println("Close() mutex acquired")

	if aw.Closed() {
		return syscall.EINVAL
	}

	atomic.StoreInt32(&aw.closed, 1)
	aw.pendingWg.Wait() // wait for pending writes to complete
	close(aw.stopCh)    // signal response listener stop

	return nil
}
