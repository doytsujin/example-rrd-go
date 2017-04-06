#!/bin/bash

cd $( dirname "$0" )
rm -f data/*.{rrd,png} || exit $?
go build -ldflags -s test-rrd.go || exit $?
time ./test-rrd
