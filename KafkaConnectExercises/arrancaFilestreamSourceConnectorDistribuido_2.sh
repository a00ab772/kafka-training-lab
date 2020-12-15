#!/bin/bash

variable=$(sudo docker ps | grep 3030 | awk '{print $1}')
echo """
7) Ejecuta los siguientes comandos: 

sudo docker exec -it $variable bash

echo -e 'log linea 1\nlog linea 2\nlog linea 3' >> /tmp/demo-file.txt

kafka-console-consumer --topic demo-2-distributed --from-beginning --bootstrap-server 127.0.0.1:9092

"""
