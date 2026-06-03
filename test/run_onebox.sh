#!/bin/bash

set -e
set -o pipefail

ONEBOX_DIR="$(readlink -f $(dirname $0))/onebox"
CONF_FILE="${ONEBOX_DIR}/conf.yaml"

function check_dependencies() {
    ver=$(python3 -V 2>&1 | sed 's/.* \([0-9]\).\([0-9]\).*/\1\2/')
    if [ "$ver" -lt "35" ]; then
        echo "python 3.5 or greater is required"
        exit 1
    fi

    if ! command -v bvc &> /dev/null; then
        echo "bvc is required"
        return 1
    fi

    if ! command -v sd &> /dev/null; then
        echo "consul is required"
        return 1
    fi
}

if ! check_dependencies; then
    echo "failed to check dependencies"
    exit 1
fi

echo "installing requirements at $ONEBOX_DIR/requirements.txt"
python3 -m pip install -r $ONEBOX_DIR/requirements.txt --index-url=https://bytedpypi.byted.org/simple >/dev/null

case $1 in
 "")
        CONF_FILE="${ONEBOX_DIR}/local.yaml"
        ;;
 local)
        CONF_FILE="${ONEBOX_DIR}/local.yaml"
        ;;
 remote)
        CONF_FILE="${ONEBOX_DIR}/remote.yaml"
        ;;
 *)
        echo "Usage: $0 [local|remote]"
        exit 1
        ;;
esac

export ASAN_OPTIONS=detect_leaks=false,abort_on_error=true
export PATH=${PATH}:output/
export PATH=${PATH}:output/third

if [ $1 == "local" ]; then
    echo "compile thrift"
    export THRIFT_PATH=${ONEBOX_DIR}/bcache2_thrift
    mkdir -p ${THRIFT_PATH}
    touch ${THRIFT_PATH}/__init__.py
    thrift -r -out ${THRIFT_PATH} --gen py src/thrift/server.thrift
fi

echo "run tests"
nosetests --nologcapture --with-id -xvs $ONEBOX_DIR/${2} 2>&1 --tc-file=$CONF_FILE --tc-format=yaml 2>&1 | tee -i onebox.output
