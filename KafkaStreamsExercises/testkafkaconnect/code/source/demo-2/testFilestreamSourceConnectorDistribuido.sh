#!/bin/bash

kafka-topics --create --topic demo-2-distributed \
     --partitions 3 --replication-factor 1 \
     --zookeeper 127.0.0.1:2181

kafka-topics --describe --topic demo-2-distributed --zookeeper 127.0.0.1:2181

echo """

3) Ve a http://formacionkafka:3030 -> Connect UI

4) Crea un conector nuevo -> File Source

5) Pega la siguiente configuracion:
"""

cat /tutorial/source/demo-2/file-stream-demo-distributed.properties

echo """

6) A continuacion, escribe el comando "exit" 

exit

7) y lanza el siguiente comando:

./arrancaFilestreamSourceConnectorDistribuido_2.sh

"""

exit 
