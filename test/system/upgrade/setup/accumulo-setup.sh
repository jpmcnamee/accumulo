#!/usr/bin/env bash

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#Default configuration ports are overwritten with the following
ZOOKEEPER_CLIENT_PORT=60109
MASTER_CLIENT_PORT=60110
TSERVER_CLIENT_PORT=60111
GC_CLIENT_PORT=60112
TRACE_CLIENT_PORT=60113
MONITOR_CLIENT_PORT=60114
MONITOR_LOG4J_PORT=60115

#Unpack and configure both old(1.5.*) and new(1.6.*) accumulo files

TEMP_DIR=$1
ACCUMULO_OLD_TAR=$2
ACCUMULO_OLD_DIR=$3
ACCUMULO_NEW_TAR=$4
ACCUMULO_NEW_DIR=$5
CURR_LOCATION=$6
HADOOP_VERSION=$7
DEFAULT_ZOOKEEPER_CLIENT_PORT=$8

if [[ ! -d $TEMP_DIR ]]; then
    mkdir $TEMP_DIR
fi

if [[ ! -z $DEFAULT_ZOOKEEPER_CLIENT_PORT ]]; then
    ZOOKEEPER_CLIENT_PORT=$DEFAULT_ZOOKEEPER_CLIENT_PORT
fi
if [[ ! -d $TEMP_DIR/$ACCUMULO_OLD_DIR ]]; then
    mkdir $TEMP_DIR/$ACCUMULO_OLD_DIR
    tar -C $TEMP_DIR/ -xzf $ACCUMULO_OLD_TAR
fi

if [[ ! -d $TEMP_DIR/$ACCUMULO_NEW_DIR ]]; then
    mkdir $TEMP_DIR/$ACCUMULO_NEW_DIR
    tar -C $TEMP_DIR/ -xzf $ACCUMULO_NEW_TAR
fi

cp $CURR_LOCATION/conf/accumulo/* $TEMP_DIR/$ACCUMULO_OLD_DIR/conf
cp $CURR_LOCATION/conf/accumulo/* $TEMP_DIR/$ACCUMULO_NEW_DIR/conf

if [[ "${HADOOP_VERSION}" = "1" ]]; then
    sed -e 's/^test -z \"$HADOOP_CONF_DIR\"/#test -z \"$HADOOP_CONF_DIR\"/' \
        -e 's/^# test -z "$HADOOP_CONF_DIR"/test -z \"$HADOOP_CONF_DIR\"/' \
        $CURR_LOCATION/conf/accumulo/accumulo-env.template.sh > $CURR_LOCATION/conf/accumulo/temp

    cp $CURR_LOCATION/conf/accumulo/temp $TEMP_DIR/$ACCUMULO_OLD_DIR/conf/accumulo-env.sh
    cp $CURR_LOCATION/conf/accumulo/temp $TEMP_DIR/$ACCUMULO_NEW_DIR/conf/accumulo-env.sh

    sed -e "/\$HADOOP_PREFIX\/share\/hadoop\/common\/\.\*\.jar,/ i <!--" \
        -e "/\$ACCUMULO_HOME\/server\/,/ i -->" \
        $CURR_LOCATION/conf/accumulo/accumulo-site.template.xml > $CURR_LOCATION/conf/accumulo/temp

    cp $CURR_LOCATION/conf/accumulo/temp $TEMP_DIR/$ACCUMULO_OLD_DIR/conf/accumulo-site.xml
    cp $CURR_LOCATION/conf/accumulo/temp $TEMP_DIR/$ACCUMULO_NEW_DIR/conf/accumulo-site.xml

    rm -f $CURR_LOCATION/conf/accumulo/temp

elif [[ "${HADOOP_VERSION}" = "2" ]]; then

    cp $CURR_LOCATION/conf/accumulo/accumulo-env.template.sh $CURR_LOCATION/conf/accumulo/accumulo-env.sh
    cp $CURR_LOCATION/conf/accumulo/accumulo-env.template.sh $CURR_LOCATION/conf/accumulo/accumulo-env.sh

    cp $CURR_LOCATION/conf/accumulo/accumulo-site.template.xml $CURR_LOCATION/conf/accumulo/accumulo-site.xml
    cp $CURR_LOCATION/conf/accumulo/accumulo-site.template.xml $CURR_LOCATION/conf/accumulo/accumulo-site.xml

else
    echo "Unkown Hadoop version: ${HADOOP_VERSION}"
    exit 1
fi


sed -e "s/\${ZOOKEEPER_CLIENT_PORT}/$ZOOKEEPER_CLIENT_PORT/" \
    -e "s=\${DFS_INSTANCE_DIR}=/accumuloTest=" \
    -e "s/\${MASTER_CLIENT_PORT}/$MASTER_CLIENT_PORT/" \
    -e "s/\${TSERVER_CLIENT_PORT}/$TSERVER_CLIENT_PORT/" \
    -e "s/\${GC_CLIENT_PORT}/$GC_CLIENT_PORT/" \
    -e "s/\${TRACE_CLIENT_PORT}/$TRACE_CLIENT_PORT/" \
    -e "s/\${MONITOR_CLIENT_PORT}/$MONITOR_CLIENT_PORT/" \
    -e "s/\$MONITOR_LOG4J_PORT/$MONITOR_LOG4J_PORT/" \
    $TEMP_DIR/$ACCUMULO_OLD_DIR/conf/accumulo-site.xml > $CURR_LOCATION/conf/accumulo/temp

cp $CURR_LOCATION/conf/accumulo/temp $TEMP_DIR/$ACCUMULO_OLD_DIR/conf/accumulo-site.xml
cp $CURR_LOCATION/conf/accumulo/temp $TEMP_DIR/$ACCUMULO_NEW_DIR/conf/accumulo-site.xml
rm -f $CURR_LOCATION/conf/accumulo/temp
echo "accumulo-site.xml configured"

