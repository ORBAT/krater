package krater

import (
	"fmt"
	"io"
	"strconv"
	"sync"

	"github.com/ORBAT/krater/kafkaconsumer"
)

var grIdGen = sequentialIntGen()

type GroupReader struct {
	started  bool
	group    string
	sub      kafkaconsumer.Subscription
	zkConn   string
	cgConf   *kafkaconsumer.Config
	closeCh  chan chan error
	writeMut sync.Mutex
	log      StdLogger
}

func NewGroupReader(group string, topics []string, zookeeper string, cgConf *kafkaconsumer.Config) (gr *GroupReader, err error) {
	if cgConf == nil {
		cgConf = kafkaconsumer.NewConfig()
	}

	// this _must_ be false since the consumer's error channel is never read from in kafkaconsumer
	cgConf.Config.Consumer.Return.Errors = false
	if err = cgConf.Validate(); err != nil {
		return
	}

	log := newLogger(fmt.Sprintf("%s (%s)", "grprd-"+strconv.Itoa(grIdGen()), group), nil)

	log.Println("Created")

	return &GroupReader{
		group:   group,
		sub:     kafkaconsumer.TopicSubscription(topics...),
		zkConn:  zookeeper,
		cgConf:  cgConf,
		log:     log,
		closeCh: make(chan chan error, 1),
	}, nil
}

// WriteTo joins the consumer group and starts consuming from its topics.
func (gr *GroupReader) WriteTo(w io.Writer) (n int64, err error) {
	gr.writeMut.Lock()
	defer gr.writeMut.Unlock()
	cg, err := kafkaconsumer.Join(gr.group, gr.sub, gr.zkConn, gr.cgConf)
	if err != nil {
		gr.log.Printf("Couldn't join consumer group: %s", err)
		return
	}

	go func() {
		if cerr, ok := <-cg.Errors(); ok && cerr != nil {
			gr.log.Printf("Consumer gave us an error: %s", cerr)
			err = cerr
			gr.Close()
		}
	}()

	msgCh := cg.Messages()

msgLoop:
	for {
		select {
		case msg := <-msgCh:
			nw, werr := w.Write(msg.Value)
			if werr != nil {
				err = werr
				break msgLoop
			}
			n += int64(nw)
		case errCh := <-gr.closeCh:
			errCh <- cg.Close()
			break msgLoop
		}
	}

	return
}

func (gr *GroupReader) Close() (err error) {
	ch := make(chan error)
	gr.closeCh <- ch
	err = <-ch
	return
}
