package krater

import (
	"bytes"
	"strconv"
	"testing"

	"github.com/bradfitz/iter"

	"gopkg.in/Shopify/sarama.v1"

	kcon "github.com/ORBAT/krater/kafkaconsumer"
)

type fakeConsumer struct {
	outMsgs  chan *sarama.ConsumerMessage
	errCh    chan error
	ackCh    chan *sarama.ConsumerMessage
	msgs     chan *sarama.ConsumerMessage
	ackCount int
	nMsgs    int
}

func genMsgs(n int, topic string) (msgs []*sarama.ConsumerMessage, nBytes int) {
	msgs = make([]*sarama.ConsumerMessage, n)
	for i := range iter.N(n) {
		val := []byte("msgval" + strconv.Itoa(i))
		nBytes += len(val)
		msgs[i] = &sarama.ConsumerMessage{Value: val, Topic: topic, Partition: int32(i), Offset: int64(i)}
	}
	return
}

func newFakeConsumer(chsz int, t testing.TB) *fakeConsumer {
	fc := &fakeConsumer{
		outMsgs: make(chan *sarama.ConsumerMessage, chsz),
		errCh:   make(chan error, chsz),
		ackCh:   make(chan *sarama.ConsumerMessage, chsz),
		msgs:    make(chan *sarama.ConsumerMessage, chsz),
	}

	go func() { // verify that acks are done in the same order the messages were received in
		for msg := range fc.ackCh {
			fc.ackCount += 1
			select {
			case expMsg := <-fc.msgs:
				if expMsg != msg {
					t.Errorf("expected message %+v but got %+v", expMsg, msg)
				}
			default:
				t.Errorf("Message %+v was unexpectedly acked", msg)
			}
			if fc.ackCount >= fc.nMsgs {
				t.Logf("Received %d acks, nMsgs %d, closing msg chan", fc.ackCount, fc.nMsgs)
				close(fc.outMsgs)
			}
		}
	}()

	return fc
}

func (fc *fakeConsumer) addMsgs(msgs []*sarama.ConsumerMessage) {
	fc.nMsgs += len(msgs)
	for _, msg := range msgs {
		fc.outMsgs <- msg
		fc.msgs <- msg
	}
}

func (fc *fakeConsumer) Ack(msg *sarama.ConsumerMessage) {
	fc.ackCh <- msg
}

func (fc *fakeConsumer) addError(err error) {
	fc.errCh <- err
}

func (_ fakeConsumer) Interrupt() {}

func (fc *fakeConsumer) Close() error {
	close(fc.ackCh)
	close(fc.errCh)
	close(fc.outMsgs)
	return nil
}

func (fc *fakeConsumer) Messages() <-chan *sarama.ConsumerMessage {
	return fc.outMsgs
}

func (fc *fakeConsumer) Errors() <-chan error {
	return fc.errCh
}

var _ kcon.Consumer = &fakeConsumer{}

const (
	testGroup = "group1"
)

func newReader(fc kcon.Consumer) *GroupReader {
	if gr, err := NewGroupReader(testGroup, []string{testTopic}, "bla:1234", nil); err != nil {
		panic(err)
	} else {
		gr.newCg = func() (kcon.Consumer, error) {
			return fc, nil
		}
		return gr
	}

}

func TestOkWriteTo(t *testing.T) {
	// defer LogTo(os.Stderr)()
	fc := newFakeConsumer(20, t)
	msgs, nBytes := genMsgs(10, testTopic)
	fc.addMsgs(msgs)

	gr := newReader(fc)
	var buf bytes.Buffer
	n, err := gr.WriteTo(&buf)

	if err != nil {
		t.Errorf("WriteTo got error %+v", err)
	}

	if n != int64(nBytes) {
		t.Errorf("Expected a total byte count of %d but got %d", nBytes, n)
	}

	t.Logf("buf %+v", buf.String())

}
