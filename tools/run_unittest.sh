#!/bin/bash

set -e

export BYTED_HOST_IP="127.0.0.1" # For server setup

script_dir=$(cd $(dirname $0) && pwd)

build_dir=$1
if [[ -z $build_dir ]];
then
    workspace=$(dirname "$script_dir") 
    build_dir="$workspace/build"
fi

export ASAN_OPTIONS=detect_leaks=false,abort_on_error=true

echo "-----Running ctest in $build_dir/src-----"
# these cases are CPU-intensive, so we run these individually to avoid CPU race
ISOLATED_CASES='PartitionReplicatorTest.StringModuleLoop|PartitionReplicatorTest.HashModuleLoop'
cd $build_dir/src && ctest --output-on-failure -j16 --repeat until-pass:3 --exclude-regex ${ISOLATED_CASES}
# TODO(wangtai.10): remove until-pass
cd $build_dir/src && ctest --output-on-failure --tests-regex ${ISOLATED_CASES} --repeat until-pass:3

echo "-----Running ctest in $build_dir/test-----"
cd $build_dir/test && ctest --output-on-failure -j16 --repeat until-pass:3
