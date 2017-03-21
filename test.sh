#!/bin/bash

cd $( dirname "$0" )
rm -f data/*.{rrd,png}
go run test-rrd.go

