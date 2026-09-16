#!/bin/bash
# set -x
# trap read debug
# set -o verbose \
#     -o xtrace

KEYPASSWORD=test1234
STOREPASSWORD=test1234
PEMPASSWORD=test1234
VALIDITY=365
DOMAIN_NAME="CN=localhost,OU=architecture,O=hcl,L=valencia,ST=valencia,C=es"
PEM_DOMAIN_NAME="/C=es/ST=valencia/L=valencia/O=hcl/OU=architecture/CN=localhost"

rm /tmp/kafka.server.truststore.jks
rm /tmp/kafka.client.keystore.jks
rm /tmp/ca-key
rm /tmp/ca-cert
rm /tmp/kafka.client.truststore.jks
rm /tmp/kafka.server.keystore.jks
rm /tmp/cert-file
rm /tmp/ca-cert.srl
rm /tmp/cert-signed

keytool -genkey -v -validity $VALIDITY -keystore /tmp/kafka.server.keystore.jks -alias localhost --dname $DOMAIN_NAME -storepass $STOREPASSWORD -keypass $KEYPASSWORD
openssl req -new -x509 -keyout /tmp/ca-key -out /tmp/ca-cert -days $VALIDITY -passout pass:$PEMPASSWORD -subj $PEM_DOMAIN_NAME
keytool -keystore /tmp/kafka.server.truststore.jks -alias CARoot -import -file /tmp/ca-cert -storepass $STOREPASSWORD -noprompt
keytool -keystore /tmp/kafka.client.truststore.jks -alias CARoot -import -file /tmp/ca-cert -storepass $STOREPASSWORD -noprompt
keytool -keystore /tmp/kafka.server.keystore.jks -alias localhost -certreq -file /tmp/cert-file -storepass $STOREPASSWORD -noprompt
openssl x509 -req -CA /tmp/ca-cert -CAkey /tmp/ca-key -in /tmp/cert-file -out /tmp/cert-signed -days $VALIDITY -CAcreateserial -passin pass:$KEYPASSWORD
keytool -keystore /tmp/kafka.server.keystore.jks -alias CARoot -import -file /tmp/ca-cert -storepass $STOREPASSWORD -noprompt
keytool -keystore /tmp/kafka.server.keystore.jks -alias localhost -import -file /tmp/cert-signed -storepass $STOREPASSWORD -noprompt
keytool -genkey -v -validity $VALIDITY -keystore /tmp/kafka.client.keystore.jks -alias localhost --dname $DOMAIN_NAME -storepass $STOREPASSWORD -keypass $KEYPASSWORD
keytool -keystore /tmp/kafka.client.keystore.jks -alias localhost -certreq -file /tmp/cert-file -storepass $STOREPASSWORD -noprompt
openssl x509 -req -CA /tmp/ca-cert -CAkey /tmp/ca-key -in /tmp/cert-file -out /tmp/cert-signed -days $VALIDITY -CAcreateserial -passin pass:$KEYPASSWORD
keytool -keystore /tmp/kafka.client.keystore.jks -alias CARoot -import -file /tmp/ca-cert -storepass $STOREPASSWORD -noprompt
keytool -keystore /tmp/kafka.client.keystore.jks -alias localhost -import -file /tmp/cert-signed -storepass $STOREPASSWORD -noprompt







