#!/bin/bash

cd /usr/local/intellij/ProyectosIntelliJ/testkafkaconnect/code/

###############
# A) FileStreamSourceConnector en modo standalone
# source/demo-1/*

echo """

Prueba del conector FileStreamSourceConnector en modo standalone.

1) Abriendo sesión remota...

2) Lanza el comando

/tutorial/source/demo-1/testFilestreamSourceConnectorStandalone.sh

"""

# Arrancar un edge node con esta carpeta en Docker
sudo docker run --rm -it -v "$(pwd)":/tutorial --net=host landoop/fast-data-dev:cp3.3.0 bash


