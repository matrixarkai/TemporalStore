#!/bin/bash  
  
cd $(dirname $0)/../

for file in $(find src/ -regex ".*\.cc\|.*\.h" | xargs)
do   
  bytelinter format -f ${file}
done  
