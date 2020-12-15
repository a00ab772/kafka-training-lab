#!/bin/bash

variable=$(sudo docker ps | grep 3030 | awk '{print $1}')

# 1) Get Worker information
sudo docker exec -it $variable curl -s 127.0.0.1:8083/ | jq

# 2) List Connectors available on a Worker
sudo docker exec -it $variable curl -s 127.0.0.1:8083/connector-plugins | jq

# 3) Ask about Active Connectors
sudo docker exec -it $variable curl -s 127.0.0.1:8083/connectors | jq

# 4) Get information about a Connector Tasks and Config
sudo docker exec -it $variable curl -s 127.0.0.1:8083/connectors/source-twitter-distributed/tasks | jq

# 5) Get Connector Status
sudo docker exec -it $variable curl -s 127.0.0.1:8083/connectors/sink-elastic-twitter-distributed/status | jq

# 6) Pause / Resume a Connector (no response if the call is succesful)
sudo docker exec -it $variable curl -s -X PUT 127.0.0.1:8083/connectors/sink-elastic-twitter-distributed/pause
sudo docker exec -it $variable curl -s -X PUT 127.0.0.1:8083/connectors/sink-elastic-twitter-distributed/resume

# 7) Get Connector Configuration
sudo docker exec -it $variable curl -s 127.0.0.1:8083/connectors/sink-postgres-twitter-distributed | jq

# 8) Delete our Connector
sudo docker exec -it $variable curl -s -X DELETE 127.0.0.1:8083/connectors/sink-elastic-twitter-distributed

# 9) Create a new Connector

sudo docker exec -it $variable curl -s -X POST -H "Content-Type: application/json" --data '{"name": "file-stream-demo-distributed", "config":{"connector.class":"org.apache.kafka.connect.file.FileStreamSourceConnector","key.converter.schemas.enable":"true","file":"demo-file.txt","tasks.max":"1","value.converter.schemas.enable":"true","name":"file-stream-demo-distributed","topic":"demo-2-distributed","value.converter":"org.apache.kafka.connect.json.JsonConverter","key.converter":"org.apache.kafka.connect.json.JsonConverter"}}' http://127.0.0.1:8083/connectors | jq

# 10) Update a connector


sudo docker exec -it $variable curl -s -X PUT -H "Content-Type: application/json" --data '{"connector.class":"org.apache.kafka.connect.file.FileStreamSourceConnector","key.converter.schemas.enable":"true","file":"demo-file.txt","tasks.max":"2","value.converter.schemas.enable":"true","name":"file-stream-demo-distributed","topic":"demo-2-distributed","value.converter":"org.apache.kafka.connect.json.JsonConverter","key.converter":"org.apache.kafka.connect.json.JsonConverter"}' 127.0.0.1:8083/connectors/file-stream-demo-distributed/config | jq



