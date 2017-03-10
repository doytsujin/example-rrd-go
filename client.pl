#!/usr/bin/env perl

use YAML::Tiny;
use JSON;
use Net::AMQP::RabbitMQ;

my $s = YAML::Tiny->read("settings.yml")->[0];

my $mq = Net::AMQP::RabbitMQ->new();
$mq->connect($s->{amqp}->{host}, {
	user => $s->{amqp}->{user},
	password => $s->{amqp}->{pass},
	port => $s->{amqp}->{port} || 5672
});
$mq->channel_open(1);
$mq->queue_declare(1, $s->{amqp}->{queue}, {durable=>1, auto_delete=>0});

my $t = time;
for (my $i=0; $i<60; $i++) {
	my $v = $i / 60.0;
	my $payload = {
		"at" => $t + $i,
		"values" => [
			$v,
			$v * $v,
			$v * $v * $v
		]
	};
	$mq->publish(1, $s->{amqp}->{queue}, JSON->new->encode($payload));
}

$mq->disconnect();
