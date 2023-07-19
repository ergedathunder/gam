#!/bin/sh
cd ../src
make clean
make -j
cd ../test
make test_gauss
./test_gauss
