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
		FilePath string `yaml:"file_path"`
		Step uint `yaml:"step"`
		Heartbeat uint `yaml:"heartbeat"`
	}
}

type RrdRequest struct {
	At int64 `yaml:"at"`
	Values []float64 `yaml:"values`
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
		panic(fmt.Sprintf("%s: %s", msg, err))
	}
}

func dumpJson(title string, value interface{}) {
	j, _ := json.Marshal(value)
	log.Printf("%s : %s\n", title, string(j))
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func main() {
	var err error
	var s Settings

	data, err := ioutil.ReadFile("settings.yml")
	failOnError(err, "Failed to load settings")
	err = yaml.Unmarshal([]byte(data), &s)
	failOnError(err, "Failed to unmarshall settings")
	//dumpJson("Settings loaded\n%s\n", s)

	_ = os.Remove(s.Rrd.FilePath)

	var port uint16 = 5672
	if s.Amqp.Port != nil { port = *s.Amqp.Port }

	location := fmt.Sprintf(
		"amqp://%s:%s@%s:%d",
		s.Amqp.User,
		s.Amqp.Pass,
		s.Amqp.Host,
		port,
	)
	log.Printf(
		"Connecting to amqp://%s@%s:%d",
		s.Amqp.User,
		s.Amqp.Host,
		port,
	)
	conn, err := amqp.Dial(location)
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	_, err = ch.QueueDeclare(
		s.Amqp.Queue, // queue
		true, // durable
		false, // delete when usused
		false, // exclusive
		false, // no-wait
		nil,   // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		s.Amqp.Queue, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			var req RrdRequest
			_ = json.Unmarshal(d.Body, &req)
			dumpJson("Received a message", req)

			if !fileExists(s.Rrd.FilePath) {
				c := rrd.NewCreator(s.Rrd.FilePath, time.Unix(req.At - 1, .0), s.Rrd.Step)
				c.RRA("AVERAGE", 0.5, 1, 60)
				c.DS("value1", "GAUGE", s.Rrd.Heartbeat, .0, 1.0)
				c.DS("value2", "GAUGE", s.Rrd.Heartbeat, .0, 1.0)
				c.DS("value3", "GAUGE", s.Rrd.Heartbeat, .0, 1.0)
				err = c.Create(true)
				failOnError(err, "Failed to create RRD file")
			}

			updater := rrd.NewUpdater(s.Rrd.FilePath)
			err = updater.Update(time.Unix(req.At, .0), req.Values[0], req.Values[1], req.Values[2])
			failOnError(err, "Failed to update RRD file")
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}
