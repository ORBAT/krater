package krater

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/bradfitz/iter"

	"github.com/Shopify/sarama"
)

func newFakeProd(insz int) *fakeProd {
	return &fakeProd{in: make(chan *sarama.MessageToSend, insz), succs: make(chan *sarama.MessageToSend, 20), errs: make(chan *sarama.ProduceError, 20)}
}

type fakeProd struct {
	in       chan *sarama.MessageToSend
	succs    chan *sarama.MessageToSend
	errs     chan *sarama.ProduceError
	msgCount int32
}

func (fp *fakeProd) Input() chan<- *sarama.MessageToSend {
	return fp.in
}

func (fp *fakeProd) Successes() <-chan *sarama.MessageToSend {
	return fp.succs
}

func (fp *fakeProd) Errors() <-chan *sarama.ProduceError {
	return fp.errs
}

func (fp *fakeProd) Close() error {
	close(fp.in)
	// close(fp.succs)
	// close(fp.errs)
	var es sarama.ProduceErrors
	for e := range fp.errs {
		es = append(es, e)
	}

	if len(es) > 0 {
		return es
	}

	return nil
}

type checker func(*sarama.MessageToSend) bool

var nopChk = func(*sarama.MessageToSend) bool { return true }

func (fp *fakeProd) popToSucc(c checker, t testing.TB) {
	msg := <-fp.in
	if !c(msg) {
		t.Errorf("Invalid message %+v", msg)
	}
	fp.succs <- msg
	atomic.AddInt32(&fp.msgCount, 1)
}

func (fp *fakeProd) popToErr(e error, c checker, t testing.TB) {
	msg := <-fp.in
	if !c(msg) {
		t.Errorf("Invalid message %+v", msg)
	}
	fp.errs <- &sarama.ProduceError{Err: e, Msg: msg}
	atomic.AddInt32(&fp.msgCount, 1)
}

func (fp *fakeProd) allSucc(c checker, t testing.TB) {
	for msg := range fp.in {
		if !c(msg) {
			t.Errorf("Invalid message %+v", msg)
		}
		fp.succs <- msg
		atomic.AddInt32(&fp.msgCount, 1)
	}
}

func (fp *fakeProd) allErr(e error, c checker, t testing.TB) {
	for msg := range fp.in {
		if !c(msg) {
			t.Errorf("Invalid message %+v", msg)
		}

		fp.errs <- &sarama.ProduceError{Err: e, Msg: msg}
		atomic.AddInt32(&fp.msgCount, 1)
	}
}

func (fp *fakeProd) count() int32 {
	return atomic.LoadInt32(&fp.msgCount)
}

var _ Producer = &fakeProd{}

func mustValue(e sarama.Encoder) []byte {
	if bs, err := e.Encode(); err != nil {
		panic(err)
	} else {
		return bs
	}
}

func safeClose(c io.Closer, t testing.TB) {
	if err := c.Close(); err != nil {
		t.Error(err)
	}
}

var testBytes = []byte{0, 1, 2, 3, 4}

var tbLen = len(testBytes)

var testTopic = "blarr"

var testErr = errors.New("dohoi")

func defChk(m *sarama.MessageToSend) bool {
	return m.Key == nil && reflect.DeepEqual(mustValue(m.Value), testBytes) && m.Topic == testTopic
}

func TestOkAckingWrite(t *testing.T) {

	kp := newFakeProd(10)
	aw := NewAckingWriter(testTopic, kp, 1)
	defer safeClose(aw, t)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		n, err := aw.Write(testBytes)
		if n != tbLen {
			t.Errorf("Wrote %d bytes, expected %d", n, tbLen)
		}

		if err != nil {
			t.Error(err)
		}
		wg.Done()
	}()
	kp.popToSucc(defChk, t)
	wg.Wait()
}

func TestOkUnsafeWrite(t *testing.T) {

	kp := newFakeProd(1)
	uw := NewUnsafeWriter(testTopic, kp)
	defer safeClose(uw, t)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		n, err := uw.Write(testBytes)
		if n != tbLen {
			t.Errorf("Wrote %d bytes, expected %d", n, tbLen)
		}

		if err != nil {
			t.Error(err)
		}
		wg.Done()
	}()
	kp.popToSucc(defChk, t)
	wg.Wait()
}

func TestBadAckingWrite(t *testing.T) {
	kp := newFakeProd(1)
	aw := NewAckingWriter(testTopic, kp, 1)
	defer safeClose(aw, t)
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		n, err := aw.Write(testBytes)
		if n != 0 {
			t.Errorf("Wrote %d bytes, expected 0", n)
		}

		if err == nil {
			t.Error("No error?")
		}
		wg.Done()
	}()

	kp.popToErr(testErr, defChk, t)
	wg.Wait()
}

func TestClosedAckingWrite(t *testing.T) {
	kp := newFakeProd(1)
	aw := NewAckingWriter(testTopic, kp, 1)

	if err := aw.Close(); err != nil {
		t.Fatal(err)
	}

	n, err := aw.Write(testBytes)
	if n != 0 {
		t.Errorf("Wrote %d bytes, expected 0", n)
	}

	if err != syscall.EINVAL {
		t.Error("Unexpected error", err)
	}
}

func TestDoubleUnsafeClose(t *testing.T) {
	kp := newFakeProd(1)
	uw := NewUnsafeWriter(testTopic, kp)

	if err := uw.Close(); err != nil {
		t.Fatal("Unexpected error", err)
	}

	if err := uw.Close(); err != syscall.EINVAL {
		t.Fatal("Unexpected error", err)
	}
}

func TestDoubleAckingClose(t *testing.T) {
	kp := newFakeProd(1)
	aw := NewAckingWriter(testTopic, kp, 1)

	if err := aw.Close(); err != nil {
		t.Fatal("Unexpected error", err)
	}

	if err := aw.Close(); err != syscall.EINVAL {
		t.Fatal("Unexpected error", err)
	}
}

func TestClosedUnsafeWrite(t *testing.T) {
	kp := newFakeProd(1)
	uw := NewUnsafeWriter(testTopic, kp)

	if err := uw.Close(); err != nil {
		t.Fatal(err)
	}

	n, err := uw.Write(testBytes)
	if n != 0 {
		t.Errorf("Wrote %d bytes, expected 0", n)
	}

	if err != syscall.EINVAL {
		t.Error("Unexpected error", err)
	}
}

func TestAckingReadFrom(t *testing.T) {
	r := bytes.NewReader(testBytes)
	kp := newFakeProd(1)
	aw := NewAckingWriter(testTopic, kp, 1)

	defer safeClose(aw, t)
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		n, err := aw.ReadFrom(r)
		if n != int64(tbLen) {
			t.Errorf("Wrote %d bytes, expected %d", n, tbLen)
		}

		if err != nil {
			t.Error(err)
		}
		wg.Done()
	}()
	kp.popToSucc(defChk, t)
	wg.Wait()
}

func TestUnsafeReadFrom(t *testing.T) {
	r := bytes.NewReader(testBytes)
	kp := newFakeProd(1)
	uw := NewUnsafeWriter(testTopic, kp)

	defer safeClose(uw, t)
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		n, err := uw.ReadFrom(r)
		if n != int64(tbLen) {
			t.Errorf("Wrote %d bytes, expected %d", n, tbLen)
		}

		if err != nil {
			t.Error(err)
		}
		wg.Done()
	}()
	kp.popToSucc(defChk, t)
	wg.Wait()
}

// TestAckingGracefulClose tests that AckingWriter finishes pending Writes when Close is called.
func TestAckingGracefulClose(t *testing.T) {
	nMsgs := 10
	kp := newFakeProd(nMsgs)
	aw := NewAckingWriter(testTopic, kp, nMsgs)

	var doneWg, startedWg sync.WaitGroup

	doneWg.Add(nMsgs)
	startedWg.Add(nMsgs)
	for _ = range iter.N(nMsgs) {
		go func() {
			startedWg.Done()
			n, err := aw.Write(testBytes)
			if n != tbLen {
				t.Errorf("Wrote %d bytes, expected %d", n, tbLen)
			}

			if err != nil {
				t.Error(err)
			}
			doneWg.Done()
		}()
	}
	startedWg.Wait()

	closedCh := make(chan struct{})

	go func() {
		time.Sleep(50 * time.Millisecond)
		if err := aw.Close(); err != nil {
			t.Error(err)
		}
		close(closedCh)
	}()
	time.Sleep(80 * time.Millisecond)
	go kp.allSucc(defChk, t)
	doneWg.Wait()

	if kp.count() != int32(nMsgs) {
		t.Fatalf("%d messages delivered to Successes(), expected %d", kp.count(), nMsgs)
	}
}

// TestUnsafeGracefulClose tests that UnsafeWriter finishes pending Writes when Close is called.
func TestUnsafeGracefulClose(t *testing.T) {
	nMsgs := 10
	kp := newFakeProd(nMsgs)
	aw := NewUnsafeWriter(testTopic, kp)

	var doneWg, startedWg sync.WaitGroup

	doneWg.Add(nMsgs)
	startedWg.Add(nMsgs)
	for _ = range iter.N(nMsgs) {
		go func() {
			startedWg.Done()
			n, err := aw.Write(testBytes)

			if n != tbLen {
				t.Errorf("Wrote %d bytes, expected %d", n, tbLen)
			}

			if err != nil {
				t.Error(err)
			}
			doneWg.Done()
		}()
	}
	startedWg.Wait()

	go func() {
		time.Sleep(50 * time.Millisecond)
		if err := aw.Close(); err != nil {
			t.Error(err)
		}
	}()
	time.Sleep(80 * time.Millisecond)
	go kp.allSucc(defChk, t)
	doneWg.Wait()

	// wait a little bit more so kp.allSucc has a chance to do stuff. Brittle and stupid but Works For Me™
	time.Sleep(80 * time.Millisecond)

	if kp.count() != int32(nMsgs) {
		t.Fatalf("%d messages delivered to Successes(), expected %d", kp.count(), nMsgs)
	}
}
