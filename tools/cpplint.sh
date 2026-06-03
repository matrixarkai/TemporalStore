#!/bin/bash

cd $(dirname $0)
cd ..

./tools/cpplint.py --linelength=100 --recursive src
