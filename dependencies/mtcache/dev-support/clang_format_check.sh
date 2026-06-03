#!/bin/bash

CUR_DIR=`dirname "$0"`
MTCACHE_HOME=`cd "$CUR_DIR"/..; pwd`

cd ${MTCACHE_HOME}

bash format_code.sh --scope origin

git diff --quiet

result=$?
if [ $result -eq 0 ]
then
    exit 0
else
    echo "clang-format code style check failed, please fix and recommit."
    exit 1
fi
