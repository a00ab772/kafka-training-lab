#!/bin/bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

pushd $DIR/../externals/librdkafka
patch -p1 < $DIR/librdkafka.patch
