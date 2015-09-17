/*
to_kafka reads delimited data from stdin and writes it to Kafka.

Usage

Supported command line options are

    to_kafka [OPTIONS] topic

  Application Options:
    -b, --brokers=   Broker addresses (can be given multiple times) (localhost:9092)
    -d, --delimiter= Delimiter byte to use when reading stdin. Remember to either use single quotes around the option or quote the
                     backslash (\\n instead of \n) if using escape sequences (\n)
    -v, --verbose    Print extra debugging cruft to stderr

  Help Options:
    -h, --help       Show this help message

  Arguments:
    topic:           Topic to write to



To read newline-delimited data from stdin and write it to topic 'sometopic':
  to_kafka sometopic

Piping a file to Kafka:
  cat somefile|to_kafka sometopic

Use comma as a delimiter:
	echo -n 'herp,derp,durr'|to_kafka -d, sometopic

*/
package main

import (
	"bufio"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	"github.com/ORBAT/krater"
	"gopkg.in/Shopify/sarama.v1"

	f "github.com/jessevdk/go-flags"
)

// Char represents a single-byte character given via the command line. It can marshal itself to either a character like ',' (sans quotes) or an escape
// sequence like '\t', and it can unmarshal either escape sequences or plain 'ol characters.
type Char byte

// MarshalFlag marshals the Char into a string, which can be either a single character or an escape sequence
func (c *Char) MarshalFlag() (s string, err error) {
	s = fmt.Sprintf("%+q", byte(*c))[1:] // the output of %+q has ' characters at the start and end, so remove those
	s = s[:len(s)-1]
	return
}

// UnmarshalFlag tries to unmarshal value into a Char. value must be either a single character or an escape sequence sequence that produces a
// single byte like \n (on UN*X) or \x00.
func (c *Char) UnmarshalFlag(value string) (err error) {
	if len(value) > 1 && value[0] != '\\' {
		return fmt.Errorf("Delimiter (%s) must be either 1 character or an escape sequence like \\n or \\x00", value)
	}

	uq, err := strconv.Unquote(`"` + value + `"`)
	if err != nil {
		return
	}

	if len(uq) != 1 {
		return fmt.Errorf("Delimiter ('%s') produced a multibyte result", value)
	}

	*c = Char(uq[0])

	return
}

var opts struct {
	Brokers   []string `short:"b" long:"brokers" description:"Broker addresses (can be given multiple times)" default:"localhost:9092"`
	Delimiter Char     `short:"d" long:"delimiter" default:"\\n" description:"Delimiter byte to use when reading stdin. Remember to either use single quotes around the option or quote the backslash (\\\\n instead of \\n) if using escape sequences"`
	Verbose   bool     `short:"v" long:"verbose" description:"Print extra debugging cruft to stderr"`
	Args      struct {
		Topic string `name:"topic" description:"Topic to write to"`
	} `positional-args:"true" required:"true"`
}

func main() {
	if _, err := f.Parse(&opts); err != nil {
		os.Exit(1)
	}

	if opts.Verbose {
		flag := log.Ldate | log.Lmicroseconds | log.Lshortfile
		log.SetFlags(flag)
		log.SetOutput(os.Stderr)
		sarama.Logger = log.New(os.Stderr, "[Sarama] ", flag)
		krater.LogTo(os.Stderr)
	} else {
		log.SetOutput(ioutil.Discard)
	}

	dels, _ := opts.Delimiter.MarshalFlag()
	log.Printf("Delimiter: '%s' (%#x), brokers: %s\ntopic: %s\n", dels, byte(opts.Delimiter), strings.Join(opts.Brokers, ", "), opts.Args.Topic)

	pc := sarama.NewConfig()

	pc.Producer.Return.Successes = true
	pc.Producer.Return.Errors = true
	pc.Producer.RequiredAcks = sarama.WaitForLocal

	kp, err := sarama.NewAsyncProducer(opts.Brokers, pc)

	if err != nil {
		fmt.Println("Error creating producer:", err.Error())
		os.Exit(2)
	}

	writer := krater.NewAckingWriter(opts.Args.Topic, kp, 1)

	termCh := make(chan os.Signal, 1)
	signal.Notify(termCh, syscall.SIGINT, syscall.SIGTERM)

	stopCh := make(chan struct{})

	r := bufio.NewReader(os.Stdin)

	defer func() {
		if err := writer.Close(); err != nil {
			panic(err)
		}
	}()

	go func() {
		totalN := int64(0)
		for {
			n, err := readAndPub(r, byte(opts.Delimiter), writer)
			totalN += int64(n)

			if err != nil {
				if err != io.EOF {
					fmt.Printf("Error after %d bytes: %s\n", totalN, err.Error())
				}
				fmt.Printf("Wrote %d bytes to Kafka\n", totalN)
				close(stopCh)
				return
			}
		}
	}()

	select {
	case sig := <-termCh:
		fmt.Println("\ngot signal", sig.String())
	case <-stopCh:
	}
}

// TODO(ORBAT): use bufio.Scanner instead of bufio.Reader

func readAndPub(r *bufio.Reader, delim byte, p io.Writer) (n int, err error) {

	line, err := r.ReadBytes(delim)
	if len(line) > 0 && line[len(line)-1] == delim {
		line = line[:len(line)-1] // remove delimiter
	}
	if len(line) == 0 {
		return
	}
	if err != nil && err != io.EOF {
		return
	}
	n, err = p.Write(line)
	return
}
