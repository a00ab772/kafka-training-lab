#!/bin/bash

echo """

# Minimum config parameters
name=sink-postgres-twitter-distributed
connector.class=io.confluent.connect.jdbc.JdbcSinkConnector

# Parallelism: 3 consumers
tasks.max=3
topics=demo-3-twitter

# Input topic has a schema, enabling it
key.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=true
value.converter=org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable=true

# See here JDBCSink connector configuration details
# http://docs.confluent.io/3.2.0/connect/connect-jdbc/docs/sink_config_options.html

connection.url=jdbc:postgresql://postgres:5432/postgres
connection.user=postgres
connection.password=postgres
insert.mode=upsert

# La clave primaria de postgres será offset + partition de Kafka
pk.mode=kafka
# Estos valores vienen por defecto
pk.fields=__connect_topic,__connect_partition,__connect_offset

# Campos de la tabla
fields.whitelist=id,created_at,text,lang,is_retweet
auto.create=true
auto.evolve=true


"""
