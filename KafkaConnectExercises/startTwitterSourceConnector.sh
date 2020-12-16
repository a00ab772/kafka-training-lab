#!/bin/bash

variable=$(docker ps | grep 3030 | awk '{print $1}')
echo """
1) Open a Twitter account if you don't have one and create a new Twitter application

https://apps.twitter.com/  and choose "New App".

You have moer details here if you need help:

https://github.com/Eneco/kafka-connect-twitter#creating-a-twitter-application

2) Run the following commands:

sudo docker exec -it $variable kafka-topics --create --topic demo-3-twitter \
    --partitions 3 --replication-factor 1 --zookeeper 127.0.0.1:2181
sudo docker exec -it $variable kafka-topics --describe --topic demo-3-twitter --zookeeper 127.0.0.1:2181

3) From the Landoop UI, create a new Twitter source connector and paste this in the configuration window,
replacing the credentials appropiately by your's:

# Minimum config parameters
name=source-twitter-distributed
connector.class=com.eneco.trading.kafka.connect.twitter.TwitterSourceConnector
tasks.max=1
topic=demo-3-twitter
key.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=true
value.converter=org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable=true
# Twitter connector config
twitter.consumerkey=0000ux4qfqaaatAsulxtAmR66d6R9999
twitter.consumersecret=0000cVTas28KihCaa4eSWvR0EbAqvhOoP1FypGQqQqPmod8v2YJWI9999
twitter.token=00004866875793-Mte3fy0GPaa8MqRXt91Vpao5AMSFFMmzu0t7y9999
twitter.secret=0000ZdSUgBQzDQeN6MJNoDDmaaFNkWvKqo2lsNAmsRaGUvN9999
# Terms you're tracking with the connector
track.terms=programming,java,kafka,scala,hcl
language=en

4) Run this to see the topic content from the command line

sudo docker exec -it $variable kafka-console-consumer --bootstrap-server localhost:9092 --topic demo-3-twitter --from-beginning
"""
