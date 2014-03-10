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

#Default ports which are overwritten
FS_DEFAULT_NAME_PORT=60101
DFS_DATANODE_ADDRESS_PORT=60102
DFS_DATANODE_HTTP_ADDRESS_PORT=60103
DFS_DATANODE_IPC_ADDRESS_PORT=60104
DFS_NAMENODE_HTTP_ADDRESS_PORT=60105
DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_PORT=60106
MAPREDUCE_TASKTRACKER_HTTP_ADDRESS_PORT=60107
MAPREDUCE_JOBTRACKER_HTTP_ADDRESS_PORT=60108

#Hadoop set-up steps for version 2.2.0
Hadoop2_Set_Up () {
    HADOOP_TAR=$1
    TEMP_DIR=$2
    HADOOP_DIR=$3
    CURR_LOCATION=$4
    HADOOP_CONF_DIR=$5

    if [[ ! -d $TEMP_DIR ]]; then
        mkdir $TEMP_DIR
    fi

    #extract tarball
    if [[ ! -d $TEMP_DIR/$HADOOP_DIR ]]; then
          mkdir $TEMP_DIR/$HADOOP_DIR
          tar -C $TEMP_DIR/ -xzf $HADOOP_TAR
    fi

    mkdir -p $TEMP_DIR/yarn/yarn_data/hdfs/namenode
    mkdir -p $TEMP_DIR/yarn/yarn_data/hdfs/datanode

    #configure yarn-site.xml
    grep -q -m 1 ShuffleHandler $HADOOP_CONF_DIR/yarn-site.xml
    if [[ "$?" != "0" ]]; then
        cp $CURR_LOCATION/conf/hadoop-2/yarn-site.template.xml $HADOOP_CONF_DIR/yarn-site.xml
        echo "yarn-site.xml configured"
    fi

    #configure core-site.xml
    grep -q -m 1 "${FS_DEFAULT_NAME_PORT}" $HADOOP_CONF_DIR/core-site.xml
    if [[ "$?" != "0" ]]; then
        sed -e "s/\${FS_DEFAULT_NAME_PORT}/$FS_DEFAULT_NAME_PORT/" \
            -e "s=\${HADOOP_TEMP_DIR}=$TEMP_DIR/temp/hadoop-2-tmp=" \
            $CURR_LOCATION/conf/hadoop-2/core-site.template.xml > $HADOOP_CONF_DIR/temp

        mv $HADOOP_CONF_DIR/temp $HADOOP_CONF_DIR/core-site.xml
        echo "core-site.xml configured"
    fi

    #configure hdfs-site.xml
    grep -q -m 1 "${DFS_DATANODE_ADDRESS_PORT}" $HADOOP_CONF_DIR/hdfs-site.xml
    if [[ "$?" != "0" ]]; then

        sed -e "s=\${DFS_NAMENODE_DIR}=$TEMP_DIR/yarn/yarn_data/hdfs/namenode=" \
            -e "s=\${DFS_DATANODE_DIR}=$TEMP_DIR/yarn/yarn_data/hdfs/datanode=" \
            -e "s/\${DFS_DATANODE_ADDRESS_PORT}/$DFS_DATANODE_ADDRESS_PORT/" \
            -e "s/\${DFS_DATANODE_HTTP_ADDRESS_PORT}/$DFS_DATANODE_HTTP_ADDRESS_PORT/" \
            -e "s/\${DFS_DATANODE_IPC_ADDRESS_PORT}/$DFS_DATANODE_IPC_ADDRESS_PORT/" \
            -e "s/\${DFS_NAMENODE_HTTP_ADDRESS_PORT}/$DFS_NAMENODE_HTTP_ADDRESS_PORT/" \
            -e "s/\${DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_PORT}/$DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_PORT/" \
            $CURR_LOCATION/conf/hadoop-2/hdfs-site.template.xml > $HADOOP_CONF_DIR/temp

        mv $HADOOP_CONF_DIR/temp $HADOOP_CONF_DIR/hdfs-site.xml
        echo "hdfs-site.xml configured"
    fi

    #configure mapred-site.xml
    if [[ ! -e $HADOOP_CONF_DIR/mapred-site.xml ]]; then

        sed -e "s/\${MAPREDUCE_TASKTRACKER_HTTP_ADDRESS_PORT}/$MAPREDUCE_TASKTRACKER_HTTP_ADDRESS_PORT/" \
            -e "s/\${MAPREDUCE_JOBTRACKER_HTTP_ADDRESS_PORT}/$MAPREDUCE_JOBTRACKER_HTTP_ADDRESS_PORT/" \
            $CURR_LOCATION/conf/hadoop-2/mapred-site.template.xml > $HADOOP_CONF_DIR/temp

        mv $HADOOP_CONF_DIR/temp $HADOOP_CONF_DIR/mapred-site.xml
        echo "mapred-site.xml configured"
    fi

    #format namenode
    if [[ ! -e $TEMP_DIR/yarn/yarn_data/hdfs/namenode/current/VERSION ]]; then
        $TEMP_DIR/$HADOOP_DIR/bin/hadoop namenode -format
    fi

    #set hadoop PID_DIR
    grep -q -m 1 "$TEMP_DIR/temp" $TEMP_DIR/$HADOOP_DIR/sbin/hadoop-daemon.sh
    if [[ "$?" != "0" ]]; then
        sed "s=HADOOP_PID_DIR\=/tmp=HADOOP_PID_DIR\=$TEMP_DIR/temp=" $TEMP_DIR/$HADOOP_DIR/sbin/hadoop-daemon.sh > $TEMP_DIR/$HADOOP_DIR/sbin/temp
        mv $TEMP_DIR/$HADOOP_DIR/sbin/temp $TEMP_DIR/$HADOOP_DIR/sbin/hadoop-daemon.sh
        chmod 777 $TEMP_DIR/$HADOOP_DIR/sbin/hadoop-daemon.sh
        echo "hadoop-daemon.sh configured"
    fi

    #set yarn PID_DIR
    grep -q -m 1 "$TEMP_DIR/temp" $TEMP_DIR/$HADOOP_DIR/sbin/yarn-daemon.sh
    if [[ "$?" != "0" ]]; then
        sed "s=YARN_PID_DIR\=/tmp=YARN_PID_DIR\=$TEMP_DIR/temp=" $TEMP_DIR/$HADOOP_DIR/sbin/yarn-daemon.sh > $TEMP_DIR/$HADOOP_DIR/sbin/temp
        mv $TEMP_DIR/$HADOOP_DIR/sbin/temp $TEMP_DIR/$HADOOP_DIR/sbin/yarn-daemon.sh
        chmod 655 $TEMP_DIR/$HADOOP_DIR/sbin/yarn-daemon.sh
        echo "yarn-daemon.sh configured"
    fi
    return 0
}

#Hadoop set-up steps for version 1.2.1
Hadoop1_Set_Up () {
    HADOOP_TAR=$1
    TEMP_DIR=$2
    HADOOP_DIR=$3
    CURR_LOCATION=$4
    HADOOP_CONF_DIR=$5

    if [[ ! -d $TEMP_DIR ]]; then
        mkdir $TEMP_DIR
    fi

    #extract tarball
    if [[ ! -d $TEMP_DIR/$HADOOP_DIR ]]; then
          mkdir $TEMP_DIR/$HADOOP_DIR
          tar -C $TEMP_DIR/ -xzf $HADOOP_TAR
    fi

    #configure core-site.xml
    grep -q -m 1 "${FS_DEFAULT_NAME_PORT}" $HADOOP_CONF_DIR/core-site.xml
    if [[ "$?" != "0" ]]; then
        sed -e "s/\${FS_DEFAULT_NAME_PORT}/$FS_DEFAULT_NAME_PORT/" \
            -e "s=\${HADOOP_TEMP_DIR}=$TEMP_DIR/temp/hadoop-1-tmp=" \
            $CURR_LOCATION/conf/hadoop-1/core-site.template.xml > $HADOOP_CONF_DIR/temp

        mv $HADOOP_CONF_DIR/temp $HADOOP_CONF_DIR/core-site.xml
        echo "core-site.xml configured"
    fi

    #configure hdfs-site.xml
    grep -q -m 1 "${DFS_DATANODE_ADDRESS_PORT}" $HADOOP_CONF_DIR/hdfs-site.xml
    if [[ "$?" != "0" ]]; then

        sed -e "s/\${DFS_DATANODE_ADDRESS_PORT}/$DFS_DATANODE_ADDRESS_PORT/" \
            -e "s/\${DFS_DATANODE_HTTP_ADDRESS_PORT}/$DFS_DATANODE_HTTP_ADDRESS_PORT/" \
            -e "s/\${DFS_DATANODE_IPC_ADDRESS_PORT}/$DFS_DATANODE_IPC_ADDRESS_PORT/" \
            -e "s/\${DFS_NAMENODE_HTTP_ADDRESS_PORT}/$DFS_NAMENODE_HTTP_ADDRESS_PORT/" \
            -e "s/\${DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_PORT}/$DFS_NAMENODE_SECONDARY_HTTP_ADDRESS_PORT/" \
            $CURR_LOCATION/conf/hadoop-1/hdfs-site.template.xml > $HADOOP_CONF_DIR/temp

        mv $HADOOP_CONF_DIR/temp $HADOOP_CONF_DIR/hdfs-site.xml
        echo "hdfs-site.xml configured"
    fi

    #configure mapred-site.xml
    grep -q -m 1 "${MAPREDUCE_TASKTRACKER_HTTP_ADDRESS_PORT}" $HADOOP_CONF_DIR/mapred-site.xml
    if [[ "$?" != "0" ]]; then

        sed -e "s/\${MAPREDUCE_TASKTRACKER_HTTP_ADDRESS_PORT}/$MAPREDUCE_TASKTRACKER_HTTP_ADDRESS_PORT/" \
            -e "s/\${MAPREDUCE_JOBTRACKER_HTTP_ADDRESS_PORT}/$MAPREDUCE_JOBTRACKER_HTTP_ADDRESS_PORT/" \
            $CURR_LOCATION/conf/hadoop-1/mapred-site.template.xml > $HADOOP_CONF_DIR/temp

        mv $HADOOP_CONF_DIR/temp $HADOOP_CONF_DIR/mapred-site.xml
        echo "mapred-site.xml configured"
    fi

    #format namenode
    if [[ ! -e $TEMP_DIR/temp/hadoop-1-tmp/dfs/name/current/VERSION ]]; then
        $TEMP_DIR/$HADOOP_DIR/bin/hadoop namenode -format
    fi

    #set hadoop PID_DIR
    grep -q -m 1 "$TEMP_DIR/temp" $HADOOP_CONF_DIR/hadoop-env.sh
    if [[ "$?" == "1" ]]; then
        sed "s=# export HADOOP_PID_DIR\=/var/hadoop/pids=HADOOP_PID_DIR\=$TEMP_DIR/temp=" $HADOOP_CONF_DIR/hadoop-env.sh > $HADOOP_CONF_DIR/temp
        mv $HADOOP_CONF_DIR/temp $HADOOP_CONF_DIR/hadoop-env.sh
        chmod 655 $HADOOP_CONF_DIR/hadoop-env.sh
        echo "hadoop-env.sh configured"
    fi

    return 0
}

#Main
HADOOP_TAR=$1
TEMP_DIR=$2
HADOOP_DIR=$3
CURR_LOCATION=$4
HADOOP_VERSION=$5

HADOOP_CONFIGURATION_SUCCESS=

if [[ "${HADOOP_VERSION}" == "1" ]]; then
    echo "Running setup for hadoop version 1"
    HADOOP_CONF_DIR="$TEMP_DIR/$HADOOP_DIR/conf"

    Hadoop1_Set_Up $HADOOP_TAR $TEMP_DIR $HADOOP_DIR $CURR_LOCATION $HADOOP_CONF_DIR
    HADOOP_CONFIGURATION_SUCCESS=$?
elif [[ "${HADOOP_VERSION}" == "2" ]]; then
    echo "Running setup for hadoop version 2"
    HADOOP_CONF_DIR="$TEMP_DIR/$HADOOP_DIR/etc/hadoop"

    Hadoop2_Set_Up $HADOOP_TAR $TEMP_DIR $HADOOP_DIR $CURR_LOCATION $HADOOP_CONF_DIR
    HADOOP_CONFIGURATION_SUCCESS=$?
else
    echo "Unkown hadoop version: $HADOOP_VERSION"
    exit 1
fi

exit $HADOOP_CONFIGURATION_SUCCESS
