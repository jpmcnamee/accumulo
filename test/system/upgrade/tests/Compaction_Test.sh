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

function verify_pass {
    RESULT=$1
    if [[ "${RESULT}" != "0" ]]; then
        echo -e "Error in Compaction Test"
        exit 1;
    fi
}


function Pre_Test {
    TTEMP_DIR=$1
    ACCUMULO_OLD_DIR=$2
    TEST_RESULTS_FILE=$3
    ERROR_RESULTS_FILE=$4


    echo -e "\n=== Running Compaction Test ===\n"

    echo -e "\n=== Compaction Test Results ===\n" >> $TEST_RESULTS_FILE 2>> $ERROR_RESULTS_FILE

    echo -e "\n=== Load 100,000 records into the test_ingest table ===\n" >> $TEST_RESULTS_FILE 2>> $ERROR_RESULTS_FILE
    $TEMP_DIR/$ACCUMULO_OLD_DIR/bin/accumulo org.apache.accumulo.test.TestIngest -u root -p secret --timestamp 1 --size 50 --random 56 --rows 100000 --start 0 --cols 1  --createTable --splits 10 >> $TEST_RESULTS_FILE 2>> $ERROR_RESULTS_FILE
    verify_pass $?

    echo -e "\n=== Load another 100,000 records into the test_ingest table ===\n" >> $TEST_RESULTS_FILE 2>> $ERROR_RESULTS_FILE
    $TEMP_DIR/$ACCUMULO_OLD_DIR/bin/accumulo org.apache.accumulo.test.TestIngest -u root -p secret --timestamp 1 --size 50 --random 56 --rows 100000 --start 100000 --cols 1  --createTable --splits 10 >> $TEST_RESULTS_FILE 2>> $ERROR_RESULTS_FILE
    verify_pass $?

    echo -e "\n=== Verify that there are 200,000 records in the test_ingest table ===\n" >> $TEST_RESULTS_FILE 2>> $ERROR_RESULTS_FILE
    $TEMP_DIR/$ACCUMULO_OLD_DIR/bin/accumulo  org.apache.accumulo.test.VerifyIngest --size 50 --timestamp 1 --random 56 --rows 200000 --start 0 --cols 1 -u root -p secret >> $TEST_RESULTS_FILE 2>> $ERROR_RESULTS_FILE
    verify_pass $?

    echo -e "\n=== Do a compaction on the test_ingest table ===\n" >> $TEST_RESULTS_FILE 2>> $ERROR_RESULTS_FILE
    echo "compact -t test_ingest -w" | $TEMP_DIR/$ACCUMULO_OLD_DIR/bin/accumulo shell -u root -p secret >> $TEST_RESULTS_FILE 2>> $ERROR_RESULTS_FILE
    verify_pass $?

}

function Post_Test {
    TEMP_DIR=$1
    ACCUMULO_NEW_DIR=$2
    TEST_RESULTS_FILE=$3
    ERROR_RESULTS_FILE=$4

    echo -e "\n=== Verify that there are 200,000 records in the test_ingest table ===\n" >> $TEST_RESULTS_FILE 2>> $ERROR_RESULTS_FILE
    $TEMP_DIR/$ACCUMULO_NEW_DIR/bin/accumulo  org.apache.accumulo.test.VerifyIngest --size 50 --timestamp 1 --random 56 --rows 200000 --start 0 --cols 1 -u root -p secret >> $TEST_RESULTS_FILE 2>> $ERROR_RESULTS_FILE
    verify_pass $?

    echo -e "\n=== Override the data Load 300,000 records into the test_ingest table ===\n" >> $TEST_RESULTS_FILE 2>> $ERROR_RESULTS_FILE
    $TEMP_DIR/$ACCUMULO_NEW_DIR/bin/accumulo org.apache.accumulo.test.TestIngest -u root -p secret --timestamp 2 --size 50 --random 56 --rows 300000 --start 0 --cols 1  --createTable --splits 10 >> $TEST_RESULTS_FILE 2>> $ERROR_RESULTS_FILE
    verify_pass $?

    echo -e "\n=== Verify that there are 300,000 records in the test_ingest table ===\n" >> $TEST_RESULTS_FILE 2>> $ERROR_RESULTS_FILE
    $TEMP_DIR/$ACCUMULO_NEW_DIR/bin/accumulo  org.apache.accumulo.test.VerifyIngest --size 50 --timestamp 2 --random 56 --rows 300000 --start 0 --cols 1 -u root -p secret >> $TEST_RESULTS_FILE 2>> $ERROR_RESULTS_FILE
    verify_pass $?
}

#Main

TEMP_DIR=$1
ACCUMULO_OLD_DIR=$2
ACCUMULO_NEW_DIR=$3
TEST_RESULTS_FILE=$4
ERROR_RESULTS_FILE=$5
TEST_PHASE=$6

if [[ "${TEST_PHASE}" = "pre" ]]; then
    Pre_Test $TEMP_DIR $ACCUMULO_OLD_DIR $TEST_RESULTS_FILE $ERROR_RESULTS_FILE
elif [[ "${TEST_PHASE}" = "post" ]]; then
    Post_Test $TEMP_DIR $ACCUMULO_NEW_DIR $TEST_RESULTS_FILE $ERROR_RESULTS_FILE
else
    echo "Invalid test phase option: $TEST_PHASE"
fi
