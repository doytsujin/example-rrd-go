#!/bin/bash

cd $( dirname "$0" )
cd data

for f in *.rrd; do
	end=$( rrdtool info "$f" | grep last_update | cut -d' ' -f3 )
	rrdtool graph "${f%.rrd}.png" --end=$end --start end-60s --width 800 \
		"DEF:value1=$f:value1:AVERAGE" \
		"DEF:value2=$f:value2:AVERAGE" \
		"DEF:value3=$f:value3:AVERAGE" \
		LINE1:value1#FF0000:"value1" \
		LINE2:value2#00FF00:"value2" \
		LINE3:value3#0000FF:"value3" &
done

wait
