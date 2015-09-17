// Package krater provides io.Writer and io.ReaderFrom implementations that produce messages to Kafka. Each Write() call will
// be written as a separate message.
//
// AckingWriter
//
// AckingWriter's Write and ReadFrom methods write messages to Kafka, blocking until a response is received from the
// broker. To allow for this, the sarama producer used to create a new AckingWriter must have Producer.Return.Successes = true
// and Producer.Return.Errors = true in their Config.
//
// Example of AckingWriter usage (error checking and imports omitted for brevity):
//
//  pc := sarama.NewConfig()
//  // these must both be true or the writer will deadlock
//  pc.Producer.Return.Successes = true
//  pc.Producer.Return.Errors = true
//
//  kp, err := sarama.NewAsyncProducer(opts.Brokers, pc)
//
//  // writer for topic "example-topic", allowing at most 10 concurrent writes
//  aw := NewAckingWriter("example-topic", kp, 10)
//  aw.Write([]byte("ahoy thar")) // this will block until Kafka responds
//
// UnsafeWriter
//
// UnsafeWriter's Write and ReadFrom methods write messages to Kafka without waiting for responses from the broker.
// Both methods will block only if the Producer's Input() channel is full. Errors are ignored.
// The following example will use Kafka as the output of the standard logger package.
//
//  pc := sarama.NewConfig()
//  // these must both be false or the writer will deadlock
//  pc.Producer.Return.Successes = false
//  pc.Producer.Return.Errors = false
//
//  kp, err := sarama.NewAsyncProducer(opts.Brokers, pc)
//
//  uw := NewUnsafeWriter("example-unsafe", kp)
//  log.New(uw, "[AHOY] ", log.LstdFlags) // create new logger that writes to uw
//  log.Println("Well this is handy")
package krater
