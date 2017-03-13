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
my @sequence = ();
my %last = ();
my $t0 = time;
for (my $id=1; $id <= $maxid; $id++) {
	$last{$id} = 0;
	for (my $t=0; $t<$len; $t++) {
		push @sequence, $id;
	}
} 
@sequence = List::Util::shuffle @sequence;

foreach my $id (@sequence) {
	my $t = $last{$id}++;
	my $v = $t / $len;
	my $payload = {
		"id" => int($id),
		"at" => $t0 + $t,
		"values" => [
			sqrt($v),
			$v,
			$v * $v
		]
	};
	$mq->publish(1, $s->{amqp}->{queue}, JSON->new->encode($payload));
}

$mq->disconnect();
