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

#Starts Hadoop and Zookeeper
function Start_PreReqs {
    HADOOP_VERSION=$1

    if [[ "${HADOOP_VERSION}" = "1" ]]; then
        $HADOOP_PREFIX/bin/start-all.sh
    elif [[ "${HADOOP_VERSION}" = "2" ]]; then
        $HADOOP_PREFIX/sbin/start-all.sh
    else
        echo -e "Unknown hadoop version: $HADOOP_VERSION"
        exit 1
    fi
    $ZOOKEEPER_HOME/bin/zkServer.sh start
}

#Stops Hadoop and Zookeeper
function Stop_PreReqs {
    HADOOP_VERSION=$1

    if [[ "${HADOOP_VERSION}" = "1" ]]; then
        $HADOOP_PREFIX/bin/stop-all.sh
    elif [[ "${HADOOP_VERSION}" = "2" ]]; then
        $HADOOP_PREFIX/sbin/stop-all.sh
    else
        echo -e "Unknown hadoop version: $HADOOP_VERSION"
        exit 1
    fi
    $ZOOKEEPER_HOME/bin/zkServer.sh stop

}

#Main

HADOOP_VERSION=$1
ACTION=$2
if [[ "${ACTION}" = "start" ]]; then
    Start_PreReqs $HADOOP_VERSION;
elif [[ "${ACTION}" = "stop" ]]; then
    Stop_PreReqs $HADOOP_VERSION;
else
    echo -e "Unkown action: $ACTION";
    exit 1;
fi
