<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# yz-flink
yz-flink is a company version based on Apache Flink
## yz-flink-1.0.1 based on flink release-1.6.0
    Fix Yarn over allocation bug
## yz-flink-1.0.2 based on flink release-1.6.1
    [FLINK-7812][metrics] Add system resources metrics
    [FLINK-10242][tests] Split StreamSourceOperatorTest \ [metrics] Disable latency metrics by default
    [FLINK-10243][metrics] Make latency metrics granularity configurable
    [FLINK-10150][metrics] Fix OperatorMetricGroup creation for Batch (Merged in 1.6.1)
    [FLINK-10105][hotfix][docs] Fixed documentation completeness test
    [FLINK-10185] Make ZooKeeperStateHandleStore#releaseAndTryRemove synchronous (Merged in 1.6.1)
    [FLINK-10011] Introduce SubmittedJobGraphStore#releaseJobGraph \ Release JobGraph after losing
        leadership in JobManager \ Release JobGraph from SubmittedJobGraphStore in Dispatcher (Merged in 1.6.1)
    [FLINK-10189] Fix inefficient use of keySet iterators (Merged in 1.6.1)
    [FLINK-10325] [State TTL] Refactor TtlListState to use only loops, no java stream API for performance (Merged in 1.6.1)
    [FLINK-10321][network] Make the condition of broadcast partitioner simple (#6688)
    [FLINK-10223][LOG]Logging with resourceId during taskmanager startup (Merged in 1.6.1)
    [FLINK-9567][yarn] Before requesting new containers always check if it is required
## yz-flink-1.0.3
    [SJJCSS-360]Specify hadoop version to 2.6.5 when compile
