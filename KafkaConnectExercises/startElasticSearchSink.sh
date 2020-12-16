#!/bin/bash

echo """

Paste these lines in the properties ElasticSearch (Confluent) sink connector box in your Landoop UI:

# Minimum config
name=sink-elastic-twitter-distributed
connector.class=io.confluent.connect.elasticsearch.ElasticsearchSinkConnector
# Parallelism to 2 tasks: consumers can scale horizontally
tasks.max=2
topics=demo-3-twitter
# Input topic has a schema, so we need to enable schema conversion with this parameter
key.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=true
value.converter=org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable=true
# ElasticSearch sink connector configuration
# http://docs.confluent.io/3.3.0/connect/connect-elasticsearch/docs/configuration_options.html
connection.url=http://elasticsearch:9200
\
# It's basically a table

type.name=kafka-connect
# Keys in Twitter are null, so we need to ignore them
key.ignore=true
# We need to add these parameter in order to have the configuration working
topic.index.map="demo-3-twitter:index1"
topic.key.ignore=true
topic.schema.ignore=true

"""
