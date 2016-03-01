package main

import (
	"io/ioutil"
	"log"
	"os"
	"os/user"

	"gopkg.in/Shopify/sarama.v1"

	"github.com/ORBAT/krater/kafkaconsumer"

	"github.com/jessevdk/go-flags"

	"github.com/ORBAT/krater"
)

var opts struct {
	Group     string   `short:"g" long:"group" description:"Consumer group name. Will default to username + _from_kafka"`
	Zookeeper string   `short:"z" long:"zookeeper" description:"Zookeeper connection string like zk1:1234,zk2:666/some/chroot" default:"localhost:2181"`
	Verbose   bool     `short:"v" long:"verbose" description:"Be verbose"`
	Topics    []string `short:"t" long:"topics" description:"Topics to consume from"`
	Delim     string   `short:"d" long:"delimiter" description:"Delimiter to use between messages" default:"\n"`
}

func main() {
	if _, err := flags.Parse(&opts); err != nil {
		os.Exit(1)
	}

	if opts.Verbose {
		flag := log.Ldate | log.Lmicroseconds | log.Lshortfile
		log.SetFlags(flag)
		log.SetOutput(os.Stderr)
		krater.LogTo(os.Stderr)
	} else {
		log.SetOutput(ioutil.Discard)
	}

	if len(opts.Group) == 0 {
		var (
			userName = "unknownuser"
			hostName = "unknownhost"
		)

		if cu, err := user.Current(); err == nil {
			userName = cu.Username
		}

		if hn, err := os.Hostname(); err == nil {
			hostName = hn
		}

		opts.Group = hostName + "_" + userName + "_from_kafka"
	}

	cgConf := kafkaconsumer.NewConfig()
	cgConf.Offsets.Initial = sarama.OffsetOldest
	gr, err := krater.NewGroupReader(opts.Group, opts.Topics, opts.Zookeeper, cgConf)
	if err != nil {
		panic(err)
	}

	n, err := gr.WriteTo(os.Stdout)
	log.Printf("%d %s", n, err)
}
