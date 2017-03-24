package main

import (
	"fmt"
	"io/ioutil"
	"log"

	"encoding/json"
	"github.com/streadway/amqp"
	"github.com/ziutek/rrd"
	"gopkg.in/yaml.v2"
	"os"
	"runtime"
	"time"
	"sync/atomic"
	"sync"
)

const (
	messageCountLimit = 6000
	channelCapacity   = 20
)

type Settings struct {
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

func panicIf(err error, msg string) {
	if err != nil {
		str := fmt.Sprintf("%s (%T) : %s", msg, err, err)
		log.Fatal(str)
		panic(str)
	}
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

var s Settings
var startTime float64 = .0
var count int64 = 0
var queue = make(map[uint]chan RrdRequest)
var acked int64 = 0
var ch *amqp.Channel
var done chan bool

type RrdRequest struct {
	Id          uint      `yaml:"id"`
	At          int64     `yaml:"at"`
	Values      []float64 `yaml:"values`
	DeliveryTag uint64
	MsgId       string
}

var createRrdFileUnleessMutex sync.Mutex

func createRrdFileUnless(req RrdRequest) string {
	createRrdFileUnleessMutex.Lock()
	defer createRrdFileUnleessMutex.Unlock()
	path := fmt.Sprintf(s.Rrd.FilePathFmt, req.Id)
	if !fileExists(path) {
		//log.Printf("Creating RRD file: %s", path)
		c := rrd.NewCreator(path, time.Unix(req.At-1, .0), s.Rrd.Step)
		c.RRA("AVERAGE", 0.5, 1, 60*60)
		c.DS("value1", "GAUGE", s.Rrd.Heartbeat, .0, 1.0)
		c.DS("value2", "GAUGE", s.Rrd.Heartbeat, .0, 1.0)
		c.DS("value3", "GAUGE", s.Rrd.Heartbeat, .0, 1.0)
		err := c.Create(true)
		panicIf(err, "Failed to create RRD file")
	}
	return path
}


func processRequest(rrdId uint) {
	var err error
	for req := range queue[rrdId] {
		path := createRrdFileUnless(req)

		//log.Printf("Updating RRD file: %s @ %d", path, req.At)
		updater := rrd.NewUpdater(path)
		for i := int64(0); i < 60; i++ {
			err = updater.Update(time.Unix(req.At+i, .0), req.Values[0], req.Values[1], req.Values[2])
		}
		panicIf(err, "Failed to update RRD file")

		currentAcked := atomic.AddInt64(&acked, 1)
		log.Printf("%d: Sending ack: id=%s tag=%x", currentAcked, req.MsgId, req.DeliveryTag)
		err := ch.Ack(req.DeliveryTag, false)
		panicIf(err, "Failed to send ack")
		if acked == messageCountLimit {
			done <- true
		}

		var dt = float64(time.Now().UnixNano())/1e9 - startTime
		currentCount := atomic.AddInt64(&count, 1)
		log.Printf("%d: %f [sec]", currentCount, dt)
	}
}

var onReceiveMutex sync.Mutex

func (req *RrdRequest) onReceive() {
	onReceiveMutex.Lock()
	defer onReceiveMutex.Unlock()
	if startTime == .0 {
		startTime = float64(time.Now().UnixNano()) / 1e9
	}
	if queue[req.Id] == nil {
		queue[req.Id] = make(chan RrdRequest, channelCapacity)
		go processRequest(req.Id)
	}
	queue[req.Id] <- *req
}

func main() {
	var err error
	var received int64 = 0
	done = make(chan bool)

	data, err := ioutil.ReadFile("settings.yml")
	panicIf(err, "Failed to load settings")
	err = yaml.Unmarshal([]byte(data), &s)
	panicIf(err, "Failed to unmarshall settings")

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
	log.Printf("Connecting to %s", location)
	conn, err := amqp.Dial(location)
	panicIf(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err = conn.Channel()
	panicIf(err, "Failed to open a channel")
	defer ch.Close()

	chOnClose := make(chan *amqp.Error)
	ch.NotifyClose(chOnClose)
	go func() {
		for err := range chOnClose {
			log.Printf("Channel closed: %+v", err)
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
	panicIf(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		s.Amqp.Queue, // queue
		"",           // consumer
		false,        // auto-ack
		false,        // exclusive
		false,        // no-local
		false,        // no-wait
		nil,          // args
	)
	panicIf(err, "Failed to register a consumer")

	go func() {
		for dlv := range msgs {
			var req RrdRequest
			err := json.Unmarshal(dlv.Body, &req)
			panicIf(err, "Failed to unmarshal message: "+string(dlv.Body))
			req.DeliveryTag = dlv.DeliveryTag
			req.MsgId = dlv.MessageId
			currentReceived := atomic.AddInt64(&received, 1)
			log.Printf("%d: Received a message: id=%s", currentReceived, dlv.MessageId)
			req.onReceive()
			runtime.Gosched()
		}
	}()

	log.Printf("Waiting for messages")
	<-done
}
