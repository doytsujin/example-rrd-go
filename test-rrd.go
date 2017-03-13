package main

import (
	"fmt"
	"log"
	"io/ioutil"

	"github.com/ziutek/rrd"
	"github.com/streadway/amqp"
	"gopkg.in/yaml.v2"
	"encoding/json"
	"os"
	"time"
)

type Settings struct {
	Amqp struct {
		Host string `yaml:"host"`
		Port *uint16 `yaml:"port"`
		User string `yaml:"user"`
		Pass string `yaml:"pass"`
		Queue string `yaml:"queue"`
	 } `yaml:"amqp"`
	Rrd struct {
		FilePathFmt string `yaml:"file_path_fmt"`
		Step uint `yaml:"step"`
		Heartbeat uint `yaml:"heartbeat"`
	}
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

var s Settings
var startTime float64 = .0
var queue = make(map[uint]chan RrdRequest)

type RrdRequest struct {
	Id uint `yaml:"id"`
	At int64 `yaml:"at"`
	Values []float64 `yaml:"values`
}

func processRequest(id uint) {
	var err error
	var path = fmt.Sprintf(s.Rrd.FilePathFmt, id)
	for req := range queue[id] {
		if !fileExists(path) {
			log.Printf("Creating RRD file: %s", path)
			c := rrd.NewCreator(path, time.Unix(req.At - 1, .0), s.Rrd.Step)
			c.RRA("AVERAGE", 0.5, 1, 60)
			c.DS("value1", "GAUGE", s.Rrd.Heartbeat, .0, 1.0)
			c.DS("value2", "GAUGE", s.Rrd.Heartbeat, .0, 1.0)
			c.DS("value3", "GAUGE", s.Rrd.Heartbeat, .0, 1.0)
			err = c.Create(true)
			failOnError(err, "Failed to create RRD file")
		}

		log.Printf("Updating RRD file: %s @ %d", path, req.At);
		updater := rrd.NewUpdater(path)
		err = updater.Update(time.Unix(req.At, .0), req.Values[0], req.Values[1], req.Values[2])
		failOnError(err, "Failed to update RRD file")

		var dt = float64(time.Now().UnixNano()) / 1e9 - startTime
		log.Printf("%f [sec]", dt);
	}
}

func (req *RrdRequest) onAmqpMessage() {
	if startTime == .0 { startTime = float64(time.Now().UnixNano()) / 1e9 }
	if queue[req.Id] == nil {
		queue[req.Id] = make(chan RrdRequest, 60)
		go processRequest(req.Id)
	}
	queue[req.Id] <- *req
}

func main() {
	var err error

	data, err := ioutil.ReadFile("settings.yml")
	failOnError(err, "Failed to load settings")
	err = yaml.Unmarshal([]byte(data), &s)
	failOnError(err, "Failed to unmarshall settings")

	var port uint16 = 5672
	if s.Amqp.Port != nil { port = *s.Amqp.Port }

	location := fmt.Sprintf(
		"amqp://%s:%s@%s:%d",
		s.Amqp.User,
		s.Amqp.Pass,
		s.Amqp.Host,
		port,
	)
	log.Printf("Connecting to %s", location)
	conn, err := amqp.Dial(location)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	_, err = ch.QueueDeclare(
		s.Amqp.Queue, // queue
		true,  // durable
		false, // delete when unused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		s.Amqp.Queue, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for msg := range msgs {
			var req RrdRequest
			_ = json.Unmarshal(msg.Body, &req)
			log.Printf("Received a message %v", req)
			req.onAmqpMessage()
		}
	}()

	log.Printf("Waiting for messages")
	<-forever
}
