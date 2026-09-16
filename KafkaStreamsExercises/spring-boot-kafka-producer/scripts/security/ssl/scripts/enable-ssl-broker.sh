#!/bin/bash
# set -x
# trap read debug

################################################################################
# Overview
################################################################################
#
################################################################################

# Source library
source ../../utils/helper.sh
source ./ssl_lib.sh

check_env || exit 1
validate_version_confluent_cli_v2 || exit 1
check_jq || exit 1

##################################################
# Initialize
##################################################

source ../config/local-demo.env
ORIGINAL_CONFIGS_DIR=/tmp/original_configs
DELTA_CONFIGS_DIR=../delta_configs
FILENAME=server.properties
create_temp_configs $CONFLUENT_HOME/etc/kafka/$FILENAME $ORIGINAL_CONFIGS_DIR/$FILENAME $DELTA_CONFIGS_DIR/${FILENAME}.delta
confluent local services kafka start


##################################################
# Cleanup
##################################################

SAVE_CONFIGS_DIR=/tmp/ssl_configs
restore_configs $CONFLUENT_HOME/etc/kafka/$FILENAME $ORIGINAL_CONFIGS_DIR/$FILENAME $SAVE_CONFIGS_DIR/${FILENAME}.ssl
