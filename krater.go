// Package krater provides io.Writer and io.ReaderFrom implementations that produce messages to Kafka.
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
