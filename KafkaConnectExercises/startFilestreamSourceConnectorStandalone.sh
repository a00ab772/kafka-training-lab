#!/bin/bash

cd KafkaConnectExercises/testkafkaconnect/code

###############
# A) FileStreamSourceConnector en modo standalone
# /tutorial/source/demo-1/*

echo """

Testing FileStreamSourceConnector in standalone mode.

1) Opening remote session ...

2) Please run the following command in the prompt line:

/tutorial/source/demo-1/testFilestreamSourceConnectorStandalone.sh

3) You have started an edge node in Docker, linking the current folder to the /tutorial remote folder

"""

# Start an edge node in Docker, linking the current folder to the /tutorial remote folder
docker run --rm -it -v "$(pwd)":/tutorial --net=host landoop/fast-data-dev:cp3.3.0 bash


