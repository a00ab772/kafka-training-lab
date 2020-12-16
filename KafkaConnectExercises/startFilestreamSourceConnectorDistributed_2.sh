#!/bin/bash

variable=$(docker ps | grep 3030 | awk '{print $1}')
echo """
8) Run the following commands:

docker exec -it $variable bash

echo -e 'log line 1\nlog line 2\nlog line 3' >> /tmp/demo-file.txt

kafka-console-consumer --topic demo-2-distributed --from-beginning --bootstrap-server 127.0.0.1:9092

"""
