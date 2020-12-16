#!/bin/bash

kafka-topics --create --topic demo-2-distributed \
     --partitions 3 --replication-factor 1 \
     --zookeeper 127.0.0.1:2181

kafka-topics --describe --topic demo-2-distributed --zookeeper 127.0.0.1:2181

echo """

3) Open this url http://formacionkafka:3030 -> Go to the Connect UI tab

4) Create a new connector of type 'File Source'

5) Paste the following config in the configuration text box:
"""

cat /tutorial/source/demo-2/file-stream-demo-distributed.properties

echo """

6) Next, write the 'exit' command

exit

7) finally, run the following command:

./startFilestreamSourceConnectorDistributed_2.sh

"""

exit 
