#!/bin/bash

end="$1"
if [ -z "$end" ]; then
	echo "USAGE: $0 <endtime in epoch>"
	exit 1
fi

rrdtool graph test.png --end="$end" --start end-60s --width 800 \
	DEF:value1=test.rrd:value1:AVERAGE \
	DEF:value2=test.rrd:value2:AVERAGE \
	DEF:value3=test.rrd:value3:AVERAGE \
	LINE1:value1#FF0000:"value1" \
	LINE2:value2#00FF00:"value2" \
	LINE3:value3#0000FF:"value3"

open test.png
