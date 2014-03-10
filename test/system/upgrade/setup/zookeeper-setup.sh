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

#ZOOKEEPER set-up steps for version 3.4.5
ZOOKEEPER_CLIENT_PORT=60109

ZOOKEEPER_TAR=$1
TEMP_DIR=$2
ZOOKEEPER_DIR=$3
CURR_LOCATION=$4
ZOOKEEPER_CONFIG_DIR=$5

if [ ! -d $TEMP_DIR ]; then
    mkdir $TEMP_DIR
fi

if [ ! -d $TEMP_DIR/$ZOOKEEPER_DIR ]; then
    tar -C $TEMP_DIR/ -xzf $ZOOKEEPER_TAR
fi

if [ ! -e $ZOOKEEPER_CONFIG_DIR/zoo.cfg ]; then
    ZOO_CONF="maxClientCnxns=100"
    sed -e "/clientPort/ i $ZOO_CONF" \
        -e "s=dataDir\=/tmp/zookeeper=dataDir\=$TEMP_DIR/temp/zookeeper=" \
        -e "s/clientPort=2181/clientPort=$ZOOKEEPER_CLIENT_PORT/" \
        $CURR_LOCATION/conf/zookeeper/zoo_sample.cfg > $ZOOKEEPER_CONFIG_DIR/zoo.cfg
    echo "zoo.cfg configured"
fi

mkdir -p $TEMP_DIR/temp/zookeeper

if [ ! -e $TEMP_DIR/temp/zookeeper/myid ]; then
    echo "13" > $TEMP_DIR/temp/zookeeper/myid
fi




