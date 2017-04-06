#!/bin/bash

cd $( dirname "$0" )
rm -f data/*.{rrd,png}
time go run -ldflags -s test-rrd.go

