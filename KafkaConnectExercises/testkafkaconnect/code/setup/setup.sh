#!/bin/bash

# 0. Asegurate que Java está instalado https://java.com/en/download/
# 1. Descarga Kafka at https://kafka.apache.org/downloads (>= 0.11.0.1)
# 2. Descomprime Kafka

# 4. Edita el archivo config/connect-distributed.config
vi config/connect-distributed.properties

# 5. Cambia bootstrap.servers a tu cluster
# 6. Cambia de rest.port=8083 a rest.port=8084 (o cualquier otro que no esté siendo utilizado)
# 7. Opcional, si quieres que el Worker pertenezca a un clúster distinto al resto de clústeres
  # 7a. offset.storage.topic=connect-offsets-2
  # 7b. config.storage.topic=connect-configs-2
  # 7c. status.storage.topic=connect-status-2
  # 7d. group.id=connect-cluster-2
# 8. Asigna la ruta plugin.path=/directory/of/your/choice
# 9. Busca en la documentación de Apache Kafka para más detalles sobre otros parámetros que pudiesen interesarte
# 10. Pon los conectores (uber jars) en la ruta plugin.path de tu elección

# 11. Para añadir más workers, duplica el connect-distributed.properties (connect-distributed-2nd-worker.properties)
# y cambia el rest.port en caso de que el Worker vaya a ejecutarse en el mismo host que el primero.

# 12. Opcionalmente, instálate el Landoop Kafka Connect UI
  # https://github.com/Landoop/kafka-connect-ui

# Por último, arrancar todos tus Kafka Connect Worker
# arturotarin@QOSMIO-X70B:~/Downloads
# 06:48:39 $ ~/Downloads/kafka_2.12-2.0.0/bin/connect-distributed.sh -daemon \
#            ~/Downloads/kafka_2.12-2.0.0/config/connect-distributed.properties

# arturotarin@QOSMIO-X70B:~/Downloads
# 06:48:39 $ ~/Downloads/kafka_2.12-2.0.0/bin/connect-distributed.sh -daemon \
#            ~/Downloads/kafka_2.12-2.0.0/config/connect-distributed-2nd-worker.properties
