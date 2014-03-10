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

function verify_success
{
    ERROR_CODE=$1
    ERROR_MESSAGE=$2
    STOP_SERVICES=$3
    STOP_ACCUMULO=$4
    ACCUMULO_TO_STOP=$5

    if [[ "${ERROR_CODE}" != "0" ]]; then
        if [[ "${STOP_ACCUMULO}" = "1" ]]; then
            $TEMP_DIR/$ACCUMULO_TO_STOP/bin/stop-all.sh
            echo -e "Accumulo: $ACCUMULO_TO_STOP has been stopped"
        fi

        if [[ "${STOP_SERVICES}" = "1" ]]; then
            #Stop Hadoop and Zookeeper
            $RESOURCES_LOCATION/services.sh $HADOOP_VERSION stop
            echo -e "Hadoop and Zookeeper have been stopped."
        fi

        echo -e "\n\n *** Failure! ***\n\nUpgrade tests did not finish successfully.\n"
        echo -e "Error with: $ERROR_MESSAGE"
        exit 1
    fi
}

function accumulo_service
{
    ACTION=$1
    ACCUMULO_DIR=$2
    TEST_RESULTS_FILE=$3
    ERROR_RESULTS_FILE=$4
    DIRTY=$5

    if [[ "${ACTION}" = "start" ]]; then
        echo -e "\n=== Starting $ACCUMULO_DIR Accumulo ===\n" >> $TEST_RESULTS_FILE 2>> $ERROR_RESULTS_FILE
        $TEMP_DIR/$ACCUMULO_DIR/bin/start-all.sh
    elif [[ "${ACTION}" = "stop" ]]; then
        if [[ "${DIRTY}" = "1" ]]; then
            echo -e "\n=== Dirty shutdown ===\n" >> $TEST_RESULTS_FILE 2>> $ERROR_RESULTS_FILE
            pkill -9 -f accumulo.start
        else
            echo -e "\n=== Normal shutdown ===\n" >> $TEST_RESULTS_FILE 2>> $ERROR_RESULTS_FILE
            $TEMP_DIR/$ACCUMULO_DIR/bin/stop-all.sh
        fi
    fi
}

function initialize_accumulo
{
    TEMP_DIR=$1
    ACCUMULO_DIR=$2
    $HADOOP_PREFIX/bin/hadoop fs -rmr /accumuloTest

    $TEMP_DIR/$ACCUMULO_DIR/bin/accumulo init --clear-instance-name --instance-name testUp --password secret
}

function usage
{
    echo -e "usage: update [-options]\nwhere options include:\n
\t-d --dir\t (Optional) Specify an alternate tmp directory to unpack and run tests\n
\t-h --hadoop\t (Required)The name of the hadoop tar file\n
\t-n --new\t (Required) specify the file name of the 1.6.* accumulo tar file\n
\t-o --old\t (Required) specify the file name of the 1.5.* accumulo tar file\n
\t-z --zookeeper\t (Required) The name of the hadoop tar file\n
\t-H --help\t print this help message\n"

}

#Main

# Start: Resolve Script Directory
SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
   bin="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
   SOURCE="$(readlink "$SOURCE")"
   [[ $SOURCE != /* ]] && SOURCE="$bin/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
bin="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
# Stop: Resolve Script Directory

CURR_LOCATION=$bin
HADOOP_TAR=
ZOOKEEPER_TAR=
ACCUMULO_OLD_TAR=
ACCUMULO_NEW_TAR=
DIRTY=0
TEMP_DIR=$CURR_LOCATION/tmp
#Execute getopt
args=$(getopt -o "d:h:n:o:z:H" -l "dir:,hadoop:,new:,old:,zookeeper:,help" -q -- "$@")

#Bad arguments
if [[ $? != 0 ]]; then
        usage
        exit 1
fi
eval set -- $args

for i
do
        case "$i" in
                -d|--dir)
                        TEMP_DIR=$2; shift
                        shift;;
                -h|--hadoop)
                        HADOOP_TAR=$2; shift
                        shift;;
                -n|--new)
                        ACCUMULO_NEW_TAR=$2; shift;
                        shift;;
                -o|--old)
                        ACCUMULO_OLD_TAR=$2; shift;
                        shift;;
                -z|--zookeeper)
                        ZOOKEEPER_TAR=$2; shift;
                        shift;;
                -H|--help)
                        usage;
                        exit 0;
                        shift;;
                --)
                        shift; break;;
        esac
done

if [[ -z $HADOOP_TAR && -z $HADOOP_PREFIX ]]; then
    echo "HADOOP_PREFIX not set. Please specify a hadoop tar file"
    exit 1
fi
if [[ -z $ZOOKEEPER_TAR && -z $ZOOKEEPER_HOME ]]; then
    echo "ZOOKEEPER_HOME not set. Please specify a zookeeper tar file"
    exit 1
fi
if [[ -z $ACCUMULO_OLD_TAR ]]; then
    echo "Please specify old accumulo tar file"
    exit 1
fi

if [[ -z $ACCUMULO_NEW_TAR ]]; then
    echo "Please specify new accumulo tar file"
    exit 1
fi

RESOURCES_LOCATION=$CURR_LOCATION/setup
TESTS_LOCATION=$CURR_LOCATION/tests
ACCUMULO_OLD_DIR=`expr match $(basename $ACCUMULO_OLD_TAR) '\(.*[^-bin\.tar\.gz]\)'`
ACCUMULO_NEW_DIR=`expr match $(basename $ACCUMULO_NEW_TAR) '\(.*[^-bin\.tar\.gz]\)'`

TEST_RESULTS_FILE=$TEMP_DIR/testResults
ERROR_RESULTS_FILE=$TEMP_DIR/errorTestResults

DEFAULT_ZOOKEEPER_CLIENT_PORT=
HADOOP_VERSION="Unknown"

if [[ ! -z $HADOOP_TAR ]]; then
    echo "Preparing to setup Hadoop"
    HADOOP_DIR=`expr match $(basename $HADOOP_TAR) '\(.*[^-bin\.tar\.gz]\)'`

    if [[ "${HADOOP_TAR}" =~ "hadoop-1" ]]; then
        HADOOP_VERSION="1"
    elif [[ "${HADOOP_TAR}" =~ "hadoop-2" ]]; then
        HADOOP_VERSION="2"
    else
        echo "Unable to determine hadoop version"
        exit 1
    fi
    #Set up Hadoop
    $RESOURCES_LOCATION/hadoop-setup.sh $HADOOP_TAR $TEMP_DIR $HADOOP_DIR $CURR_LOCATION  $HADOOP_VERSION
    verify_success $? "Hadoop setup"
    export HADOOP_PREFIX=$TEMP_DIR/$HADOOP_DIR
    echo "Hadoop setup finished"
else #Figure out the existing hadoop version
    echo "Using existing Hadoop setup"
    temp=$($HADOOP_PREFIX/bin/hadoop version)
    if [[ "${temp}" =~ "Hadoop 1" ]]; then
        HADOOP_VERSION="1"
    elif [[ "${temp}" =~ "Hadoop 2" ]]; then
        HADOOP_VERSION="2"
    else
        echo "Unable to determine hadoop version"
        exit 1
    fi
fi
if [[ ! -z $ZOOKEEPER_TAR ]]; then
    echo "Preparing to setup Zookeeper"
    ZOOKEEPER_DIR=`expr match $(basename $ZOOKEEPER_TAR) '\(.*[^-bin\.tar\.gz]\)'`
    ZOOKEEPER_CONF_DIR="$TEMP_DIR/$ZOOKEEPER_DIR/conf"
    #Set up Zookeeper
    $RESOURCES_LOCATION/zookeeper-setup.sh $ZOOKEEPER_TAR $TEMP_DIR $ZOOKEEPER_DIR $CURR_LOCATION $ZOOKEEPER_CONF_DIR
    verify_success $? "Zookeeper setup"

    export ZOOKEEPER_HOME=$TEMP_DIR/$ZOOKEEPER_DIR
    echo "Zookeeper setup finished"
else
    echo "Using existing Zookeeper setup"
    #Figure out zookeeper client port
    DEFAULT_ZOOKEEPER_CLIENT_PORT=$(grep clientPort $ZOOKEEPER_HOME/conf/zoo.cfg | cut -d = -f 2)
fi

echo "Preparing to setup Accumulo"

#Set up Accumulo
$RESOURCES_LOCATION/accumulo-setup.sh $TEMP_DIR $ACCUMULO_OLD_TAR $ACCUMULO_OLD_DIR $ACCUMULO_NEW_TAR $ACCUMULO_NEW_DIR $CURR_LOCATION $HADOOP_VERSION $DEFAULT_ZOOKEEPER_CLIENT_PORT
verify_success $? "Accumulo setup"

echo "ACCUMULO setup finished"

#Start up Hadoop and Zookeeper
$RESOURCES_LOCATION/services.sh $HADOOP_VERSION start
verify_success $? "Hadoop and Zookeeper start"
#Give hadoop and zookeeper a few seconds to startup properly
sleep 10
#Leave safe mode so that deletes in hdfs can be performed
$HADOOP_PREFIX/bin/hadoop dfsadmin -safemode leave

#Initialize output file (overwrites old file)
echo -e "=== Accumulo Upgrade Test Results ===\n" > $TEST_RESULTS_FILE
echo -e "=== Accumulo Errors ===\n" > $ERROR_RESULTS_FILE

for UPGRADE_TEST in $TESTS_LOCATION/*.sh
do
    echo "Test $UPGRADE_TEST"
    #Run the test once with normal shutdowns
    initialize_accumulo $TEMP_DIR $ACCUMULO_OLD_DIR

    accumulo_service start $ACCUMULO_OLD_DIR $TEST_RESULTS_FILE $ERROR_RESULTS_FILE 0
    $UPGRADE_TEST $TEMP_DIR $ACCUMULO_OLD_DIR $ACCUMULO_NEW_DIR $TEST_RESULTS_FILE $ERROR_RESULTS_FILE 'pre'
    verify_success $? "$UPGRADE_TEST Pre-Phase failed\nfor more info check output log: $TEST_RESULTS_FILE and error log: $ERROR_RESULTS_FILE" 1 1 $ACCUMULO_OLD_DIR
    accumulo_service stop $ACCUMULO_OLD_DIR $TEST_RESULTS_FILE $ERROR_RESULTS_FILE 0


    accumulo_service start $ACCUMULO_NEW_DIR $TEST_RESULTS_FILE $ERROR_RESULTS_FILE 0
    $UPGRADE_TEST $TEMP_DIR $ACCUMULO_OLD_DIR $ACCUMULO_NEW_DIR $TEST_RESULTS_FILE $ERROR_RESULTS_FILE 'post'
    verify_success $? "$UPGRADE_TEST Post-Phase failed\nfor more info check output log: $TEST_RESULTS_FILE and error log: $ERROR_RESULTS_FILE" 1 1 $ACCUMULO_NEW_DIR
    accumulo_service stop $ACCUMULO_NEW_DIR $TEST_RESULTS_FILE $ERROR_RESULTS_FILE 0

    #Run the test a second time with dirty shutdowns
    initialize_accumulo $TEMP_DIR $ACCUMULO_OLD_DIR

    accumulo_service start $ACCUMULO_OLD_DIR $TEST_RESULTS_FILE $ERROR_RESULTS_FILE 1
    $UPGRADE_TEST $TEMP_DIR $ACCUMULO_OLD_DIR $ACCUMULO_NEW_DIR $TEST_RESULTS_FILE $ERROR_RESULTS_FILE 'pre'
    verify_success $? "$UPGRADE_TEST Pre-Phase failed\nfor more info check output log: $TEST_RESULTS_FILE and error log: $ERROR_RESULTS_FILE" 1 1 $ACCUMULO_OLD_DIR
    accumulo_service stop $ACCUMULO_OLD_DIR $TEST_RESULTS_FILE $ERROR_RESULTS_FILE 1


    accumulo_service start $ACCUMULO_NEW_DIR $TEST_RESULTS_FILE $ERROR_RESULTS_FILE 1
    $UPGRADE_TEST $TEMP_DIR $ACCUMULO_OLD_DIR $ACCUMULO_NEW_DIR $TEST_RESULTS_FILE $ERROR_RESULTS_FILE 'post'
    verify_success $? "$UPGRADE_TEST Post-Phase failed\nfor more info check output log: $TEST_RESULTS_FILE and error log: $ERROR_RESULTS_FILE" 1 1 $ACCUMULO_NEW_DIR
    accumulo_service stop $ACCUMULO_NEW_DIR $TEST_RESULTS_FILE $ERROR_RESULTS_FILE 1


done

#Stop Hadoop and Zookeeper
$RESOURCES_LOCATION/services.sh $HADOOP_VERSION stop
verify_success $? "Hadoop and Zookeeper stop"

echo -e "\n\n*** Success! *** \n\nUpgrade tests completed hadoop zookeeper and accumulo have been stopped.\nPlease check the $TEST_RESULTS_FILE to verify the test results for correctness."
