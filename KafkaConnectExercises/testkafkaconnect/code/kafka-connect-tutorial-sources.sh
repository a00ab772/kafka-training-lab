#!/bin/bash

# 1) Source connectors
# Arranca el cluster
docker-compose up kafka-cluster
# Espera 2 minutos

###############
# A) FileStreamSourceConnector en modo standalone
# source/demo-1/*

# Arrancar un edge node con esta carpeta en Docker
docker run --rm -it -v "$(pwd)":/tutorial --net=host landoop/fast-data-dev:cp3.3.0 bash

# Desde la sesión lanzamos un conector en modo standalone y ejecutamos estos comandos:
cd /tutorial/source/demo-1

kafka-topics --create --topic demo-1-standalone \
  --partitions 3 --replication-factor 1 --zookeeper 127.0.0.1:2181

kafka-topics --describe --topic demo-1-standalone --zookeeper 127.0.0.1:2181

# El primer parámetro ha de ser la configuración del Worker
# Los siguientes parámetros pueden ser tantos conectores como queramos ejecutar en el
# Kafka Connect standalone cluster
connect-standalone -daemon /tutorial/source/demo-1/worker.properties \
  /tutorial/source/demo-1/file-stream-demo-standalone.properties

kafka-console-consumer --bootstrap-server localhost:9092 --topic demo-1-standalone --from-beginning

echo -e "log linea 1\nlog linea 2\nlog linea 3" >> demo-file.txt
###############

###############
# B) FileStreamSourceConnector en modo distribuído:

# Desde la sesión de edgenode (si no la tienes arráncala de nuevo)
# lanzamos un conector en modo standalone y ejecutamos estos comandos:
docker run --rm -it --net=host landoop/fast-data-dev:cp3.3.0 bash
kafka-topics --create --topic demo-2-distributed --partitions 3 --replication-factor 1 --zookeeper 127.0.0.1:2181

kafka-topics --describe --topic demo-2-distributed --zookeeper 127.0.0.1:2181

# Ve a http://127.0.0.1:3030 -> Connect UI
# Crea un conector nuevo -> File Source
# Pega la configuración de source/demo-2/file-stream-demo-distributed.properties

# Entra en la máquina, al estar en modo distribuído debes depositar el archivo en la misma
# máquina.
# SI LO HICIESES DESDE EL EDGENODE NO TE FUNCIONARÍA
# arturotarin@QOSMIO-X70B:~/Documents/Mistral/2019-01-08 Formación Kafka Desarrolladores/Kafka Connect
# 18:45:25 $
docker ps
# CONTAINER ID        IMAGE                          COMMAND                  CREATED             STATUS              PORTS                                                                                                                                                                                        NAMES
# 408554a5f9e1        landoop/fast-data-dev:latest   "/usr/bin/dumb-ini..."   9 hours ago         Up 23 minutes       0.0.0.0:2181->2181/tcp, 0.0.0.0:3030->3030/tcp, 0.0.0.0:9092->9092/tcp, 0.0.0.0:9581-9585->9581-9585/tcp, 3031/tcp, 0.0.0.0:8082->8081/tcp, 0.0.0.0:8083->8082/tcp, 0.0.0.0:8084->8083/tcp   code_kafka-cluster_1

# arturotarin@QOSMIO-X70B:~/Documents/Mistral/2019-01-08 Formación Kafka Desarrolladores/Kafka Connect
# 18:45:42 $
docker exec -it 408554a5f9e1 bash

# Crea el archivo demo-file.txt
echo -e "log linea 1\nlog linea 2\nlog linea 3" >> /tmp/demo-file.txt

# Lee los datos del tópico
docker run --rm -it --net=host landoop/fast-data-dev:cp3.3.0 bash
kafka-console-consumer --topic demo-2-distributed --from-beginning --bootstrap-server 127.0.0.1:9092
###############

###############
# C) TwitterSourceConnector en modo distribuido mode:
# Desde la sesión de edgenode (si no la tienes arráncala de nuevo)
# lanzamos un conector en modo standalone y ejecutamos estos comandos:
docker run --rm -it --net=host landoop/fast-data-dev:cp3.3.0 bash

kafka-topics --create --topic demo-3-twitter --partitions 3 --replication-factor 1 --zookeeper 127.0.0.1:2181
kafka-topics --describe --topic demo-3-twitter --zookeeper 127.0.0.1:2181
kafka-console-consumer --topic demo-3-twitter --bootstrap-server 127.0.0.1:9092

# Crea una nueva aplicación de twitter en tu cuenta (debes crear una cuenta de Twitter si no la tienes)
# https://github.com/Eneco/kafka-connect-twitter#creating-a-twitter-application
# https://apps.twitter.com/ y crea "New App".

# Configurar el conector: https://github.com/Eneco/kafka-connect-twitter#setup
# Rellena demo-3/source-twitter-distributed.properties con tus claves
# Arranca el conector y observa como llegan los datos al tópico
