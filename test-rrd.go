package main

import (
	"fmt"
	"io/ioutil"

	"encoding/json"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/streadway/amqp"
	"github.com/ziutek/rrd"
	"gopkg.in/yaml.v2"
)

const (
	messageCountLimit = 6000
)

type settings struct {
	Amqp struct {
		Host  string  `yaml:"host"`
		Port  *uint16 `yaml:"port"`
		User  string  `yaml:"user"`
		Pass  string  `yaml:"pass"`
		Queue string  `yaml:"queue"`
	} `yaml:"amqp"`
	Rrd struct {
		FilePathFmt string `yaml:"file_path_fmt"`
		Step        uint   `yaml:"step"`
		Heartbeat   uint   `yaml:"heartbeat"`
	}
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

var s settings
var startTime float64
var queue = make(map[uint]chan rrdRequest)
var ch *amqp.Channel
var done chan struct{}

type rrdRequest struct {
	ID          uint      `yaml:"id"`
	At          int64     `yaml:"at"`
	Values      []float64 `yaml:"values"`
	DeliveryTag uint64
	MsgID       string
}

var createRrdFileUnleessMutex sync.Mutex

func createRrdFileUnless(req rrdRequest) string {
	createRrdFileUnleessMutex.Lock()
	defer createRrdFileUnleessMutex.Unlock()
	path := fmt.Sprintf(s.Rrd.FilePathFmt, req.ID)
	if !fileExists(path) {
		//fmt.Printf("Creating RRD file: %s\n", path)
		c := rrd.NewCreator(path, time.Unix(req.At-1, .0), s.Rrd.Step)
		c.RRA("AVERAGE", 0.5, 1, 60*60)
		c.DS("value1", "GAUGE", s.Rrd.Heartbeat, .0, 1.0)
		c.DS("value2", "GAUGE", s.Rrd.Heartbeat, .0, 1.0)
		c.DS("value3", "GAUGE", s.Rrd.Heartbeat, .0, 1.0)
		err := c.Create(true)
		if err != nil {
			panic(errors.Wrap(err, "Failed to create RRD file"))
		}
	}
	return path
}

var acked int64

func processRequest(rrdID uint) {
	var err error
	for req := range queue[rrdID] {
		path := createRrdFileUnless(req)

		//fmt.Printf("Updating RRD file: %s @ %d\n", path, req.At)
		updater := rrd.NewUpdater(path)
		for i := int64(0); i < 60; i++ {
			err = updater.Update(time.Unix(req.At+i, .0), req.Values[0], req.Values[1], req.Values[2])
		}
		if err != nil {
			panic(errors.Wrap(err, "Failed to update RRD file"))
		}

		currentAcked := atomic.AddInt64(&acked, 1)
		// fmt.Printf("%d: Sending ack: id=%s tag=%x\n", currentAcked, req.MsgID, req.DeliveryTag)
		err := ch.Ack(req.DeliveryTag, false)
		if err != nil {
			panic(errors.Wrap(err, "Failed to send ack"))
		}

		fmt.Printf(" %5d\n\033[1A", currentAcked)
		if acked == messageCountLimit {
			var dt = float64(time.Now().UnixNano())/1e9 - startTime
			fmt.Printf("Sent the last ack: %f [sec]\n", dt)
			close(done)
		}
	}
}

func (req *rrdRequest) onReceive() {
	if startTime == .0 {
		fmt.Printf("Received the first message\n")
		startTime = float64(time.Now().UnixNano()) / 1e9
	}
	if queue[req.ID] == nil {
		queue[req.ID] = make(chan rrdRequest)
		go processRequest(req.ID)
	}
	queue[req.ID] <- *req
}

func handleMessages(msgs <-chan amqp.Delivery) {
	var received int64
	for dlv := range msgs {
		var req rrdRequest
		err := json.Unmarshal(dlv.Body, &req)
		if err != nil {
			panic(errors.Wrap(err, "Failed to unmarshal message: "+string(dlv.Body)))
		}
		req.DeliveryTag = dlv.DeliveryTag
		req.MsgID = dlv.MessageId
		currentReceived := atomic.AddInt64(&received, 1)
		_ = currentReceived // fmt.Printf("%d: Received a message: id=%s\n", currentReceived, dlv.MessageId)
		req.onReceive()
		runtime.Gosched()
	}
}

func main() {
	var err error
	done = make(chan struct{})

	data, err := ioutil.ReadFile("settings.yml")
	if err != nil {
		panic(errors.Wrap(err, "Failed to load settings"))
	}
	err = yaml.Unmarshal(data, &s)
	if err != nil {
		panic(errors.Wrap(err, "Failed to unmarshall settings"))
	}

	var port uint16 = 5672
	if s.Amqp.Port != nil {
		port = *s.Amqp.Port
	}

	location := fmt.Sprintf(
		"amqp://%s:%s@%s:%d",
		s.Amqp.User,
		s.Amqp.Pass,
		s.Amqp.Host,
		port,
	)
	fmt.Printf("Connecting to %s\n", location)
	conn, err := amqp.Dial(location)
	if err != nil {
		panic(errors.Wrap(err, "Failed to connect to RabbitMQ"))
	}
	defer conn.Close()

	ch, err = conn.Channel()
	if err != nil {
		panic(errors.Wrap(err, "Failed to open a channel"))
	}
	defer ch.Close()

	chOnClose := make(chan *amqp.Error)
	ch.NotifyClose(chOnClose)
	go func() {
		for err := range chOnClose {
			fmt.Printf("Channel closed: %+v\n", err)
		}
	}()

	_, err = ch.QueueDeclare(
		s.Amqp.Queue, // queue
		true,         // durable
		false,        // delete when unused
		false,        // exclusive
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		panic(errors.Wrap(err, "Failed to declare a queue"))
	}

	msgs, err := ch.Consume(
		s.Amqp.Queue, // queue
		"",           // consumer
		false,        // auto-ack
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
	)
	if err != nil {
		panic(errors.Wrap(err, "Failed to register a consumer"))
	}

	go handleMessages(msgs)

	fmt.Printf("Waiting for messages\n")
	<-done
}
