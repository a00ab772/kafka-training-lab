#!/bin/bash

# 1) Source connectors
docker-compose up kafka-cluster elasticsearch postgres
# Espera 2 minutos

###############
# A) ElasticSearch Sink
# + info http://docs.confluent.io/3.2.0/connect/connect-elasticsearch/docs/elasticsearch_connector.html
# Comprobamos que funciona
http://127.0.0.1:9200/

# Ve a http://127.0.0.1:3030 -> Connect UI
sink/demo-elastic/sink-elastic-twitter-distributed.properties

# Puedes ver los datos en Elasticsearch con el plugin Dejavu
http://127.0.0.1:9200/_plugin/dejavu

# http://docs.confluent.io/3.1.1/connect/connect-elasticsearch/docs/configuration_options.html
# Contar los tweets:
http://127.0.0.1:9200/demo-3-twitter/_count

# Se puede descargar los datos desde el UI
# Se puede consultar en elasticsearch. Por ejemplo, query-high-friends.json muestra los usuarios que más contactos tienen
###############

###############
# B) PosgresSQL JDBC Sink demo
# Fuente de estos ejemplos:
# http://docs.confluent.io/3.2.0/connect/connect-jdbc/docs/sink_connector.html#quickstart
# sink/demo-postgres/sink-postgres-twitter-distributed.sh
###############

###############
# C) REST API Demo
# Fuente de estos ejemplos:
# http://docs.confluent.io/3.2.0/connect/managing.html#common-rest-examples
# sink/demo-rest-api/demo-rest-api.sh
###############
