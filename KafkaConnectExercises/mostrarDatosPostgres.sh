#!/bin/bash

echo """

CONSULTAR EN POSTGRES
=====================
Puedes ejecutar esta secuencia de comandos:

psql -U postgres

select * from \"public\".\"demo-3-twitter\";
select count(*) from \"public\".\"demo-3-twitter\";

"""


variable=$(sudo docker ps | grep postgres | awk '{print $1}')

sudo docker exec -it $variable bash

