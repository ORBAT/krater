// Package krater provides io.Writer and io.ReaderFrom implementations that produce messages to Kafka.
//
// AckingWriter
//
// AckingWriter's Write and ReadFrom methods write messages to Kafka, blocking until a response is received from the
// broker. To allow for this, the sarama producer used to create a new AckingWriter must have AckSuccesses = true
// in their ProducerConfig.
//
// Example of AckingWriter usage (error checking and imports omitted for brevity):
//
//  client, _ := sarama.NewClient("example", []string{"localhost:6667"}, nil)
//  cfg := sarama.NewProducerConfig()
//  cfg.FlushFrequency = 100 * time.Millisecond
//  cfg.FlushMsgCount = 200 // flush at 100ms intervals or after 200 messages
//  cfg.ChannelBufferSize = 20
//  cfg.AckSuccesses = true // this has to be true or Write will block
//  prod, _ := sarama.NewProducer(client, cfg)
//
//  // write to topic "example-topic", allow at most 10 concurrent writes
//  aw := NewAckingWriter("example-topic", prod, 10)
//  aw.Write([]byte("ahoy thar"))
//
// UnsafeWriter
//
// UnsafeWriter's Write and ReadFrom methods write messages to Kafka without waiting for responses from the broker.
// Both methods will block only if the Producer's Input() channel is full. Errors are ignored (they are logged, though.)
//
//  client, _ := sarama.NewClient("example", []string{"localhost:6667"}, nil)
//  cfg := sarama.NewProducerConfig()
//  cfg.FlushFrequency = 100 * time.Millisecond
//  cfg.FlushMsgCount = 200 // flush at 100ms intervals or after 200 messages
//  cfg.ChannelBufferSize = 20
//  cfg.AckSuccesses = false // no need for this since we're going to gleefully ignore acks
//  cfg.RequiredAcks = 0     // in fact, let's ignore acks completely
//  prod, _ := sarama.NewProducer(client, cfg)
//
//  uw := NewUnsafeWriter("example-unsafe", prod)
//  log.New(uw, "[AHOY] ", log.LstdFlags)
//  log.Println("Well this is handy")
package krater

import (
	"io"

	"github.com/Shopify/sarama"
)

// The Producer interface is used by UnsafeWriter and QueuingWriter.
type Producer interface {
	Errors() <-chan *sarama.ProduceError
	Successes() <-chan *sarama.MessageToSend
	Input() chan<- *sarama.MessageToSend
	io.Closer
}
