#!/bin/bash
# set -x
# trap read debug

export CONFLUENT_HOME=~/Downloads/confluent-6.0.0

################################################################################
# Overview
################################################################################
#
# SSL authentication and encryption with Confluent
#
# Usage:
#
#   ./run.sh
#
# Requirements:
#
#   - Local install of Confluent CLI (v0.265.0 or above)
#
################################################################################

# Source library
source  ../../utils/helper.sh

check_env \
  && print_pass "Confluent Platform installed" \
  || exit 1
check_running_cp ${CONFLUENT} \
  && print_pass "Confluent Platform version ${CONFLUENT} ok" \
  || exit 1
validate_version_confluent_cli_for_cp \
  && print_pass "Confluent CLI version ok" \
  || exit 1
check_jq \
  && print_pass "jq installed" \
  || exit 1
sleep 1


./cleanup.sh
./init.sh

./generateTLSKeysAndCertificates.sh
./enable-ssl-broker.sh

echo
echo  *=====================================================================*
echo  "  This security configuration has been enabled in your Kafka cluster:"
echo  *=====================================================================*
echo
confluent local services kafka log | grep "909"
confluent local services kafka log | grep "listener.security.protocol.map ="
confluent local services kafka log | grep "advertised.listeners = TOKEN"