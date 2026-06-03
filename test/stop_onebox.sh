#!/bin/bash

tmpfile="pid.txt"

netstat -ltnp | awk  '{print $NF}' | awk  '/.\/server*|.\/bench*|.\/proxy*|.\/metaserver*/' \
                  | awk -F / '{print $1}'  |  awk BEGIN{RS=EOF}'{gsub(/\n/," ");print}' > $tmpfile


kill -9 $(<$tmpfile)



rm $tmpfile