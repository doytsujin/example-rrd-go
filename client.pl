#!/usr/bin/env perl

use strict;
use YAML::Tiny;
use JSON;
use Net::AMQP::RabbitMQ;
use List::Util;

my $s = YAML::Tiny->read("settings.yml")->[0];

my $mq = Net::AMQP::RabbitMQ->new();
$mq->connect($s->{amqp}->{host}, {
	user => $s->{amqp}->{user},
	password => $s->{amqp}->{pass},
	port => $s->{amqp}->{port} || 5672
});
$mq->channel_open(1);
$mq->queue_declare(1, $s->{amqp}->{queue}, {durable=>1, auto_delete=>0});

my $maxid = 20;
my $len = 60;
my $t0 = 1489710600;
my $msgid = 0;
for (my $t=0; $t<$len; $t++) {
	for (my $id=1; $id <= $maxid; $id++) {
		my $v = $t / $len;
		my $payload = {
			"id" => int($id),
			"at" => $t0 + $t * 60,
			"values" => [
				sqrt($v),
				$v,
				$v * $v
			]
		};
		$mq->publish(
			1,
			$s->{amqp}->{queue},
			JSON->new->encode($payload),
			undef,
			{
				"delivery_mode" => 2,
				"message_id" => "msg-" . ++$msgid
			}
		);
	}
}

$mq->disconnect();
