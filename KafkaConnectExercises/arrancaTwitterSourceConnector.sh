#!/bin/bash

variable=$(sudo docker ps | grep 3030 | awk '{print $1}')
echo """
1) Crea una nueva aplicación de twitter en tu cuenta
 (debes crear una cuenta de Twitter si no la tienes)

https://apps.twitter.com/ y crea "New App".

Si quieres más detalles de cómo hacerlo puedes leer las instrucciones en detalle aquí:

https://github.com/Eneco/kafka-connect-twitter#creating-a-twitter-application

https://apps.twitter.com/ y crea "New App".

2) Ejecuta los siguientes comandos: 

sudo docker exec -it $variable kafka-topics --create --topic demo-3-twitter \
    --partitions 3 --replication-factor 1 --zookeeper 127.0.0.1:2181
sudo docker exec -it $variable kafka-topics --describe --topic demo-3-twitter --zookeeper 127.0.0.1:2181

3) Entra en tu terminal de Landoop, crea un nuevo conector de twitter y añade esta configuración,
reemplazando mis credenciales por las tuyas

# Estos son los parámetros estandar mínimos que todos los conectores necesitarán
name=source-twitter-distributed
connector.class=com.eneco.trading.kafka.connect.twitter.TwitterSourceConnector
tasks.max=1
topic=demo-3-twitter
key.converter=org.apache.kafka.connect.json.JsonConverter
key.converter.schemas.enable=true
value.converter=org.apache.kafka.connect.json.JsonConverter
value.converter.schemas.enable=true
# Configuración específica para el conector de Twitter
twitter.consumerkey=0000ux4qfqGGXatAsulxtAmR66d6R9999
twitter.consumersecret=0000cVTas28KihCiJL4eSWvR0EbAqvhOoP1FypGQqQqPmod8v2YJWI9999
twitter.token=00004866875793-Mte3fy0GPi6F58MqRXt91Vpao5AMSFFMmzu0t7y9999
twitter.secret=0000ZdSUgBQzDQeN6MJNoDDmKCBdFNkWvKqo2lsNAmsRaGUvN9999
track.terms=programming,java,kafka,scala
language=en

4) Ejecuta este comando para ver el contenido del tópico en consola

sudo docker exec -it $variable kafka-console-consumer --bootstrap-server localhost:9092 --topic demo-3-twitter --from-beginning
"""
