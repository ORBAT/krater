package krater

import (
	"fmt"
	"io"
	"io/ioutil"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"

	"gopkg.in/Shopify/sarama.v1"
)

// type KeyerFn represents any function that can turn a message into a key for that particular message
type KeyerFn func(*sarama.ProducerMessage) sarama.Encoder

// AckingWriter is an io.Writer that writes messages to Kafka. Parallel calls to Write() will cause messages to be queued by the producer, and
// each Write() call will block until a response is received from the broker.
//
// The AsyncProducer passed to NewAckingWriter must have Config.Return.Successes == true and Config.Return.Errors == true
//
// Close() must be called when the writer is no longer needed.
type AckingWriter struct {
	kp        sarama.AsyncProducer
	id        string
	topic     string
	stopCh    chan empty // used to signal event loop close
	closed    int32      // nonzero if the writer has started closing. Must be accessed atomically
	log       StdLogger
	pendingWg sync.WaitGroup // WaitGroup for pending message responses
	sema      chan empty     // used as a counting semaphore
	closeCh   chan empty
	closeMut  sync.Mutex
	keyer     KeyerFn
}

var awIdGen = sequentialIntGen()

type empty struct{}

// NewAckingWriter returns an AckingWriter that uses kp to produce messages to Kafka topic 'topic', with a maximum of maxConcurrent concurrent writes.
//
// kp MUST have been initialized with AckSuccesses = true or Write will block indefinitely.
func NewAckingWriter(topic string, kp sarama.AsyncProducer, maxConcurrent int) *AckingWriter {
	id := "ackwr-" + strconv.Itoa(awIdGen())
	logger := newLogger(fmt.Sprintf("%s -> %s", id, topic), nil)

	logger.Printf("Created")

	aw := &AckingWriter{
		kp:      kp,
		id:      id,
		topic:   topic,
		log:     logger,
		stopCh:  make(chan empty),
		sema:    make(chan empty, maxConcurrent),
		closeCh: make(chan empty)}

	go withRecover(aw.handleErrors)
	go withRecover(aw.handleSuccesses)
	aw.keyer = func(_ *sarama.ProducerMessage) sarama.Encoder { return nil }
	return aw
}

func (aw *AckingWriter) handleSuccesses() {
	for {
		select {
		case <-aw.closeCh:
			aw.log.Println("Stopping handleSuccesses")
			return
		case msg := <-aw.kp.Successes():
			close(msg.Metadata.(chan error))
		}
	}
}

func (aw *AckingWriter) handleErrors() {
	for {
		select {
		case <-aw.closeCh:
			aw.log.Println("Stopping handleErrors")
			return
		case msg := <-aw.kp.Errors():
			msg.Msg.Metadata.(chan error) <- msg.Err
		}
	}
}

// SetKeyer sets the keyer function used to specify keys for messages. Defaults to having nil keys
// for all messages. SetKeyer is NOT thread safe, and it must not be used if any writes are underway.
func (aw *AckingWriter) SetKeyer(fn KeyerFn) {
	aw.keyer = fn
}

// TODO(orbat): Write timeouts?

// Write will queue p as a single message, blocking until a response is received. n will always be len(p) if the
// message was sent successfully, 0 otherwise. The message's key is determined by the keyer function set with SetKeyer,
// and defaults to nil.
// Trying to Write to a closed writer will return syscall.EINVAL.
//
// Thread-safe.
//
// Implements io.Writer.
func (aw *AckingWriter) Write(p []byte) (n int, err error) {
	if aw.Closed() {
		return 0, syscall.EINVAL
	}

	// acquire write semaphore or wait until we get it
	aw.sema <- empty{}
	defer func() { <-aw.sema }()

	msg := &sarama.ProducerMessage{Topic: aw.topic, Value: sarama.ByteEncoder(p)}

	msg.Key = aw.keyer(msg)

	resCh := make(chan error, 1)
	msg.Metadata = resCh

	aw.kp.Input() <- msg

	if err = <-resCh; err != nil {
		return
	}

	n = len(p)

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
	aw.log.Println("Pending writes done")
	return nil
}
