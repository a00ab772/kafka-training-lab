#!/bin/bash

cd KafkaConnectExercises/testkafkaconnect/code

###############
# B) FileStreamSourceConnector in Distributed mode
# /tutorial/source/demo-2/*

echo """

Testing the FileStreamSourceConnector in distributed mode

1) Opening a remote session ...

2) Run the following command in the prompt session:

/tutorial/source/demo-2/testFilestreamSourceConnectorDistributed.sh

"""

# Start an edge node in Docker, linking the current folder to the /tutorial remote folder
docker run --rm -it -v "$(pwd)":/tutorial --net=host landoop/fast-data-dev:cp3.3.0 bash


