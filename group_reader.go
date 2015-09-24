package krater

import (
	"errors"
	"io"
	"sync"

	"github.com/wvanbergen/kafka/kafkaconsumer"
)

var grIdGen = sequentialIntGen()

type GroupReader struct {
	started bool
	cg      kafkaconsumer.Consumer
	group   string
	sub     kafkaconsumer.Subscription
	zkConn  string
	cgConf  *kafkaconsumer.Config
	closeCh chan chan empty
	wtm     sync.Mutex
	log     StdLogger
}

func NewGroupReader(group string, topics []string, zookeeper string, cgConf *kafkaconsumer.Config) *GroupReader {
	if cgConf == nil {
		cgConf = kafkaconsumer.NewConfig()
	}

	return &GroupReader{
		group:   group,
		sub:     kafkaconsumer.TopicSubscription(topics...),
		zkConn:  zookeeper,
		cgConf:  cgConf,
		closeCh: make(chan chan empty),
	}
}

func (gr *GroupReader) WriteTo(w io.Writer) (n int64, err error) {
	gr.wtm.Lock()
	defer gr.wtm.Unlock()
	if err = gr.start(); err != nil {
		return
	}
	defer gr.stop()

	return
}

func (gr *GroupReader) Close() (err error) {
	ch := make(chan empty)
	gr.closeCh <- ch
	<-ch
	return nil
}

func (gr *GroupReader) start() (err error) {
	if gr.started {
		return errors.New("GroupReader already started")
	}

	if gr.cg, err = kafkaconsumer.Join(group, subscription, zookeeper, config); err != nil {
		return
	}

	gr.started = true
	return
}

func (gr *GroupReader) stop() error {
	if !gr.started {
		return errors.New("GroupReader not started")
	}

	return gr.cg.Close()
}
