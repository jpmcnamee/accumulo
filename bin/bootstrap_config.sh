#! /usr/bin/env bash

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

function usage
{
    echo -e "usage: bootstrap_config.sh [-options]\nwhere options include:\n
\t-d --dir\t Alternate directory to setup config files\n
\t-s --size\t Supported sizes: '1GB' '2GB' '3GB' '512MB'\n
\t-n --native\t Configure to use native libraries\n
\t-j --jvm\t Configure to use the jvm\n
\t-o --override\t Configure to use the jvm\n
\t-v --version\t Specifie the Apache Hadoop version supported versions: '1' '2'\n
\t-h --hlep\t print this help message\n"

}

# Start: Resolve Script Directory
SOURCE="${BASH_SOURCE[0]}"
while [ -h "$SOURCE" ]; do # resolve $SOURCE until the file is no longer a symlink
   bin="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
   SOURCE="$(readlink "$SOURCE")"
   [[ $SOURCE != /* ]] && SOURCE="$bin/$SOURCE" # if $SOURCE was a relative symlink, we need to resolve it relative to the path where the symlink file was located
done
bin="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
echo $bin
echo $SOURCE
# Stop: Resolve Script Directory

#
# Resolve accumulo home for bootstrapping
#
ACCUMULO_HOME=$( cd -P ${bin}/.. && pwd )
TEMPLATE_CONF_DIR="${ACCUMULO_HOME}/conf/templates"
CONF_DIR="${ACCUMULO_HOME}/conf"
ACCUMULO_SITE=accumulo-site.xml
ACCUMULO_ENV=accumulo-env.sh

SIZE=
TYPE=
HADOOP_VERSION=
OVERRIDE="0"

while [ "$1" != "" ]; do
    case $1 in
        -d | --dir)             shift
                                CONF_DIR=$1
                                ;;
        -s | --size)            shift
                                SIZE=$1
                                ;;         
        -n | --native)          TYPE=native
                                ;;
        -j | --jvm)             TYPE=jvm
                                ;;
        -o | --override)        OVERRIDE="1"
                                ;;
        -v | --version)         shift
                                HADOOP_VERSION=$1
                                ;;
        -h | --help )           usage
                                exit
                                ;;
        * )                     usage
                                exit 1
    esac
    shift
done

while [ "${OVERRIDE}" == "0" ]; do
        if [ -e "${CONF_DIR}/${ACCUMULO_ENV}" -o -e "${CONF_DIR}/${ACCUMULO_SITE}" ]; then
                echo "Warning your current config files in ${CONF_DIR} will be overwritten!"
                echo
                echo "How would you like to proceed?:"
                select CHOICE in 'Continue with overwrite' 'Specify new conf dir'; do
                   if [ "${CHOICE}" == 'Specify new conf dir' ]; then
                      echo -n "Please specifiy new conf directory: "
                      read CONF_DIR
                   else
                     OVERRIDE=1
                   fi
                   break
                done
        else 
                OVERRIDE=1
        fi
done
echo "Coppying configuration files to: ${CONF_DIR}"


#Native 1GB
native_1GB_tServer="-Xmx128m -Xms128m"
_1GB_master="-Xmx128m -Xms128m"
_1GB_monitor="-Xmx64m -Xms64m"
_1GB_gc="-Xmx64m -Xms64m"
_1GB_other="-Xmx128m -Xms64m"

_1GB_memoryMapMax="256M"
native_1GB_nativeEnabled="true"
_1GB_cacheDataSize="15M"
_1GB_cacheIndexSize="40M"
_1GB_sortBufferSize="50M"
_1GB_waLogMaxSize="256M"

#Native 2GB
native_2GB_tServer="-Xmx256m -Xms256m"
_2GB_master="-Xmx256m -Xms256m"
_2GB_monitor="-Xmx128m -Xms64m"
_2GB_gc="-Xmx128m -Xms128m"
_2GB_other="-Xmx256m -Xms64m"

_2GB_memoryMapMax="512M"
native_2GB_nativeEnabled="true"
_2GB_cacheDataSize="30M"
_2GB_cacheIndexSize="80M"
_2GB_sortBufferSize="50M"
_2GB_waLogMaxSize="512M"

#Native 3GB
native_3GB_tServer="-Xmx1g -Xms1g -XX:NewSize=500m -XX:MaxNewSize=500m"
_3GB_master="-Xmx1g -Xms1g"
_3GB_monitor="-Xmx1g -Xms256m"
_3GB_gc="-Xmx256m -Xms256m"
_3GB_other="-Xmx1g -Xms256m"

_3GB_memoryMapMax="1G"
native_3GB_nativeEnabled="true"
_3GB_cacheDataSize="128M"
_3GB_cacheIndexSize="128M"
_3GB_sortBufferSize="128M"
_3GB_waLogMaxSize="1G"

#Native 512MB
native_512MB_tServer="-Xmx48m -Xms48m"
_512MB_master="-Xmx128m -Xms128m"
_512MB_monitor="-Xmx64m -Xms64m"
_512MB_gc="-Xmx64m -Xms64m"
_512MB_other="-Xmx128m -Xms64m"

_512MB_memoryMapMax="80M"
native_512MB_nativeEnabled="true"
_512MB_cacheDataSize="7M"
_512MB_cacheIndexSize="20M"
_512MB_sortBufferSize="50M"
_512MB_waLogMaxSize="100M"

#JVM 1GB
jvm_1GB_tServer="-Xmx384m -Xms384m"

jvm_1GB_nativeEnabled="false"

#JVM 2GB
jvm_2GB_tServer="-Xmx768m -Xms768m"

jvm_2GB_nativeEnabled="false"

#JVM 3GB
jvm_3GB_tServer="-Xmx2g -Xms2g -XX:NewSize=1G -XX:MaxNewSize=1G"

jvm_3GB_nativeEnabled="false"

#JVM 512MB
jvm_512MB_tServer="-Xmx128m -Xms128m"

jvm_512MB_nativeEnabled="false"


if [ -z "${SIZE}" ]; then
   echo "Choose the heap configuration:"
   select DIRNAME in 1GB 2GB 3GB 512MB; do
      echo "Using '${DIRNAME}' configuration"
      SIZE=${DIRNAME}
      break
   done
elif [ "${SIZE}" != "1GB" -a "${SIZE}" != "2GB" -a "${SIZE}" != "3GB" -a "${SIZE}" != "512MB" ]; then
        echo "Invalid memory size"
        echo "Supported sizes: '1GB' '2GB' '3GB' '512MB'"
        exit 1
fi

if [ -z "${TYPE}" ]; then
   echo
   echo "Choose the Accumulo memory-map type:"
   select TYPENAME in Java Native; do
      if [ "${TYPENAME}" == "Native" ]; then
         TYPE="native"
         echo "Don't forget to build the native libraries using the bin/build_native_library.sh script"
      elif [ "${TYPENAME}" == "Java" ]; then
         TYPE="jvm"
      fi
      echo "Using '${TYPE}' configuration"
      echo
      break
   done
fi

if [ -z "${HADOOP_VERSION}" ]; then
   echo
   echo "Choose the Apache Hadoop version:"
   select HADOOP in 'HADOOP 1' 'HADOOP 2' ; do
      if [ "${HADOOP}" == "HADOOP 2" ]; then
         HADOOP_VERSION="2"
      elif [ "${HADOOP}" == "HADOOP 1" ]; then
         HADOOP_VERSION="1"
      fi
      echo "Using Apache Hadoop version '${HADOOP_VERSION}' configuration"
      echo
      break
   done
elif [ "${HADOOP_VERSION}" != "1" -a "${HADOOP_VERSION}" != "2" ]; then
        echo "Invalid Apache Hadoop version"
        echo "Supported Apache Hadoop versions: '1' '2'"
        exit 1
fi

if [ -z ${SIZE} ]; then
        echo "Invalid size configuration option"
        exit 1
elif [ -z ${TYPE} ]; then
        echo "Invalide type configuration option"
        exit 1
elif [ -z ${HADOOP_VERSION} ]; then
        echo "Invalid Accumulo Hadoop version configuration option"
        exit 1
fi

TSERVER="${TYPE}_${SIZE}_tServer"
MASTER="_${SIZE}_master"
MONITOR="_${SIZE}_monitor"
GC="_${SIZE}_gc"
OTHER="_${SIZE}_other"

MEMORY_MAP_MAX="_${SIZE}_memoryMapMax"
NATIVE="${TYPE}_${SIZE}_nativeEnabled"
CACHE_DATA_SIZE="_${SIZE}_cacheDataSize"
CACHE_INDEX_SIZE="_${SIZE}_cacheIndexSize"
SORT_BUFFER_SIZE="_${SIZE}_sortBufferSize"
WAL_MAX_SIZE="_${SIZE}_waLogMaxSize"

#Configure accumulo-env.sh
mkdir -p "${CONF_DIR}" && cp ${TEMPLATE_CONF_DIR}/* ${CONF_DIR}/
sed -e "s/\${tServerHigh tServerLow}/${!TSERVER}/" -e "s/\${masterHigh masterLow}/${!MASTER}/" -e "s/\${monitorHigh monitorLow}/${!MONITOR}/" -e "s/\${gcHigh gcLow}/${!GC}/" -e "s/\${otherHigh otherLow}/${!OTHER}/" ${TEMPLATE_CONF_DIR}/$ACCUMULO_ENV > ${CONF_DIR}/$ACCUMULO_ENV

#Configure accumulo-site.xml
sed -e "s/\${memMapMax}/${!MEMORY_MAP_MAX}/" -e "s/\${nativeEnabled}/${!NATIVE}/" -e "s/\${cacheDataSize}/${!CACHE_DATA_SIZE}/" -e "s/\${cacheIndexSize}/${!CACHE_INDEX_SIZE}/" -e "s/\${sortBufferSize}/${!SORT_BUFFER_SIZE}/" -e "s/\${waLogMaxSize}/${!WAL_MAX_SIZE}/" ${TEMPLATE_CONF_DIR}/$ACCUMULO_SITE > ${CONF_DIR}/$ACCUMULO_SITE

#Configure for hadoop 2
if [ "$HADOOP_VERSION" == "2" ]; then
        sed -e 's/^test -z \"$HADOOP_CONF_DIR\"/#test -z \"$HADOOP_CONF_DIR\"/' -e 's/^# test -z "$HADOOP_CONF_DIR"/test -z \"$HADOOP_CONF_DIR\"/' ${CONF_DIR}/$ACCUMULO_ENV > temp
        mv temp ${CONF_DIR}/$ACCUMULO_ENV

        ACCUMULO_SITE_INFO="\$HADOOP_PREFIX/share/hadoop/common/.*.jar,\n\t\$HADOOP_PREFIX/share/hadoop/common/lib/.*.jar,\n\t\$HADOOP_PREFIX/share/hadoop/hdfs/.*.jar,\n\t\$HADOOP_PREFIX/share/hadoop/mapreduce/.*.jar,\n\t\$HADOOP_PREFIX/share/hadoop/yarn/.*.jar,\n\t/usr/lib/hadoop/.*.jar,\n\t/usr/lib/hadoop/lib/.*.jar,\n\t/usr/lib/hadoop-hdfs/.*.jar,\n\t/usr/lib/hadoop-mapreduce/.*.jar,\n\t/usr/lib/hadoop-yarn/.*.jar,\n"

        sed -e "/\$ACCUMULO_HOME\/server\/target\/classes\/,/ i $ACCUMULO_SITE_INFO" ${CONF_DIR}/$ACCUMULO_SITE > temp
        mv temp  ${CONF_DIR}/$ACCUMULO_SITE
fi


if [ "${TYPE}" == "native" ]; then
        echo -e "Please remember to compile the native libraries using the bin/build_native_library.sh script and to set the LD_LIBRARY_PATH variable in the conf/accumulo-env.sh script"
fi
echo "Setup complete"
