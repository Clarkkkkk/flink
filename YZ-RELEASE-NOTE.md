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

## yz-flink-1.0.4
    [FLINK-10687] [table] Introduce flink-table-common
    [FLINK-9916] Add FROM_BASE64 function for table/sql API
    [FLINK-9928] Add LOG2 function for table/sql API
    [FLINK-9688] [table] Add ATAN2 SQL function support
    [FLINK-10079] [table] Look up sink tables in external catalogs.
    [FLINK-9738][table] Provide a way to define Temporal Table Functions in Table API Piotr Nowojski 2018/7/6, 1:47 AM
    [hotfix][table] Deduplicate optimize code between stream and batch table environment Piotr Nowojski 2018/8/24, 7:17 PM
    [hotfix][table] Extract computeCost in FlinkLogicalJoin to base class Piotr Nowojski 2018/7/6, 1:58 AM
    [hotfix][table] Deduplicate RelTimeInidicatoConverter logic Piotr Nowojski 2018/9/20, 7:15 PM
    [hotfix][table,tests] Reduce mockito usage in TableTestUtil Piotr Nowojski 2018/8/27, 10:49 PM
    [FLINK-9713][table][sql] Support versioned join in planning phase Piotr Nowojski 2018/7/6, 2:02 AM
    [hotfix][table] Extract DataStreamJoinToCoProcessTranslator Piotr Nowojski 2018/7/18, 12:59 AM
    [hotfix][table] Simplify NonWindowJoin class Piotr Nowojski 2018/7/19, 10:55 PM
    [hotfix][table] Add convienient constructors for CRow Piotr Nowojski 2018/7/20, 6:14 PM
    [hotfix][table,tests] Add convienient verify methods to HarnessTestBase Piotr Nowojski 2018/7/20, 6:40 PM
    [FLINK-9714][table] Support versioned joins with processing timePiotr Nowojski2018/9/20, 8:15 PM
	[hotfix][docs,table] Split Streaming Concepts page into multiple documents. Piotr Nowojski* 2018/9/19, 9:20 PM
	[FLINK-9712][docs,table] Document processing time Temporal Table JoinsPiotr Nowojski*2018/9/20, 3:43 PM
	[FLINK-10156][table] Deprecate Table.writeToSink().Fabian Hueske2018/10/8, 7:41 PM
	[FLINK-9915] [table] Add TO_BASE64 function for table/sql APIyanghua*2018/7/23, 5:52 PM
	[FLINK-9853] [table] Add HEX support for Table API & SQL xueyu* 2018/7/15, 8:01 PM
	[FLINK-7205] [table] Add UUID() for Table API & SQLwind*2018/8/1, 12:52 PM
	[FLINK-9977] [table] [docs] Refine the SQL/Table built-in function docs. Xingcan Cui* 2018/7/28, 11:55 PM
	[FLINK-10059] [table] Add LTRIM function in Table API and SQL yanghua* 2018/8/5, 5:09 PM
	[FLINK-10060] [table] Add RTRIM function in Table API and SQL yanghua* 2018/8/7, 5:17 PM
	[FLINK-10136] [table] Add REPEAT function in Table API and SQL yanghua* 2018/8/21, 8:35 PM
	[FLINK-10174] [table] Define UTF-8 charset for HEX, TO_BASE64, FROM_BASE64xueyu*2018/8/20, 3:16 PM
	[FLINK-9991] [table] Add regexp_replace function to TableAPI and SQL yanghua* 2018/7/29, 9:36 PM
	[FLINK-6846] [table] Add timestamp addition in Table API xueyu*2018/6/20, 9:30 PM
	[FLINK-6847] [FLINK-6813] [table] Add support for TIMESTAMPDIFF in Table API & SQLxueyu*2018/7/8, 6:50 PM
    [FLINK-9642][cep] Added caching layer to SharedBuffer(#6205) Aitozi* 2018/8/29, 11:08 PM
    [FLINK-10417][cep] Added option to throw exception on pattern variable miss during SKIP_TO_FIRST/LAST Dawid Wysakowicz* 2018/10/1, 3:46 PM
    [FLINK-10414][cep] Added skip to next strategy Dawid Wysakowicz* 2018/10/1, 5:40 PM
    [FLINK-10163] [sql-client] Support views in SQL Client Timo Walther* 2018/8/23, 1:03 PM
    [FLINK-10281] [table] Fix string literal escaping throughout Table & SQL API Timo Walther* 2018/9/7, 7:32 PM
    [FLINK-10474][table] Evaluate IN/NOT_IN with literals as local predicate. hequn8128* 2018/10/4, 5:26 PM
    [FLINK-7062][table][cep] Initial support for the basic functionality of MATCH_RECOGNIZE Dian Fu* 2017/8/8, 7:10 PM
    [hotfix] [table] [docs] Improvements for the functions documentation Timo Walther* 2018/8/14, 5:39 PM
    [FLINK-10187] [table] Fix LogicalUnnestRule after upgrading to Calcite 1.17. Shuyi Chen* 2018/8/21, 2:30 PM
    [FLINK-10201] [table] [test] The batchTestUtil was mistakenly used in some stream sql tests Xingcan Cui* 2018/8/23, 10:16 AM
    [FLINK-10222] [table] Fix parsing of keywords. yanghua* 2018/8/28, 5:31 PM
    [FLINK-10259] [table] Fix identification of key attributes for GroupWindows. Fabian Hueske* 2018/8/30, 9:39 PM
    [FLINK-10145] [table] Add replace function in Table API and SQL Guibo Pan* 2018/8/19, 1:09 AM
    [FLINK-10451] [table] TableFunctionCollector should handle the life cycle of ScalarFunction Xpray* 2018/9/28, 4:34 PM
    [FLINK-9990] [table] Add regex_extract function in TableAPI and SQL yanghua* 2018/7/29, 11:27 AM
    [FLINK-10528][table] Remove methods that were deprecated in Flink 1.4.0. Fabian Hueske* 2018/10/11, 8:06 PM
    [hotfix][table] Rewrite TemporalJoin from CoProcessFunction to TwoInputStreamOperator Piotr Nowojski* 2018/9/13, 7:03 PM
    [FLINK-9715][table] Support temporal join with event time Piotr Nowojski* 2018/9/7, 5:24 PM
    [hotfix][table] Allowed using '|' and stripMargin with indenter Dawid Wysakowicz* 2018/10/18, 9:10 PM
    [hotfix][cep] Throw exception when skipping to first element of a match Dawid Wysakowicz* 2018/10/9, 4:47 PM
    [hotfix][cep] Changed cep operator names to distinguish between global and keyed Dawid Wysakowicz* 2018/10/9, 4:48 PM
    [FLINK-10470] Add method to check if pattern can produce empty matches Dawid Wysakowicz* 2018/10/5, 9:24 PM
    [hotfix][cep] Added equals/hashcode to Pattern/SkipStrategies/Quantifier Dawid Wysakowicz* 2018/10/9, 4:49 PM
    [FLINK-7062][table][cep] Improved support of basic functionality of MATCH RECOGNIZE Dawid Wysakowicz* 2018/10/19, 10:36 PM
    [FLINK-10263] [sql-client] Fix classloader issues in SQL Client Timo Walther* 2018/9/20, 4:28 PM
    [FLINK-3875] [connectors] Add an upsert table sink factory for Elasticsearch Timo Walther* 2018/8/15, 7:51 PM
    [FLINK-10687] [table] Move TableException and ValidationException to flink-table-common Timo Walther 2018/10/26, 7:35 PM
    [FLINK-8865] [sql-client] Add CLI query code completion in SQL Client xueyu* 2018/10/4, 12:34 PM
    [FLINK-8865] [sql-client] Finalize CLI query code completion in SQL Client Timo Walther* 2018/10/13, 2:58 AM
    [hotfix][table,test] Deduplicate code in ExpressionTestBase Piotr Nowojski 2018/8/29, 4:51 PM
    [hotfix][table,test] Improve error message in ExpressionTestBase Piotr Nowojski 2018/8/30, 7:36 PM
    [FLINK-10398] Add Tanh math function supported in Table API and SQL yanghua* 2018/9/23, 12:47 PM
    [FLINK-10384] Add Sinh math function supported in Table API and SQL yanghua* 2018/9/21, 2:47 PM
    [FLINK-9737] [table] Add more auxiliary methods for descriptor properties Timo Walther* 2018/10/23, 3:48 PM
    [FLINK-9737] [table] Make function validation optional Timo Walther* 2018/10/22, 7:48 PM
    [FLINK-9737] [FLINK-8880] [sql-client] Support defining temporal tables in environment files Timo Walther* 2018/10/22, 7:51 PM
    [FLINK-10396] Remove CodebaseType Till Rohrmann* 2018/9/23, 1:54 AM
    [FLINK-10400] Fail JobResult if application finished in CANCELED or FAILED state Till Rohrmann* 2018/9/24, 3:09 AM
    [FLINK-10508] Port "The JobManager actor must handle jobs when not enough slots" to MiniClusterITCase#testHandleJobsWhenNotEnoughSlot 陈梓立* 2018/10/12, 1:22 PM
    [hotfix] a bit refactor of MiniClusterITCase 陈梓立* 2018/10/12, 1:27 PM
    [FLINK-10508] Port scheduling test 陈梓立* 2018/10/12, 1:44 PM
    [FLINK-10508] Port Savepoint relevant tests 陈梓立* 2018/10/13, 3:06 AM
    [FLINK-10508] Remove JobManagerITCase Till Rohrmann* 2018/10/18, 9:13 PM
    [FLINK-9900][tests] Include more information on timeout in Zookeeper HA ITCase Chesnay Schepler* 2018/8/20, 11:40 PM
    [FLINK-9900][tests] Include more information on timeout in Zookeeper HA ITCase Chesnay Schepler* 2018/8/20, 11:40 PM
    [FLINK-10397] Remove CoreOptions#MODE Till Rohrmann* 2018/9/23, 5:16 AM
    [FLINK-10637] Use MiniClusterResource for tests in flink-runtime Till Rohrmann* 2018/10/22, 10:00 PM
    Revert CoreOptions Shimin Yang 2019/1/25, 5:10 PM
    [FLINK-10675][sql-client][table] Fixed depdency issues in cep & table/sql-client integration Dawid Wysakowicz* 2018/10/25, 4:07 PM
    [hotfix][table] Added default branches to pattern matching to supress warnings Dawid Wysakowicz* 2018/10/29, 4:18 PM
    [FLINK-6670][tests] Remove CommonTestUtils#createTempDirectory Chesnay Schepler* 2018/8/20, 11:37 PM
    [FLINK-10687] [table] Move TableSchema to flink-table-common Timo Walther* 2018/10/26, 7:34 PM
    [FLINK-10687] [table] Move TypeStringUtils to flink-table-common Timo Walther* 2018/10/26, 9:57 PM
    [FLINK-10687] [table] Move DescriptorProperties to flink-table-common Timo Walther* 2018/10/29, 4:18 PM
    [FLINK-10687] [table] Move format factories to flink-table-common Timo Walther* 2018/10/27, 1:45 PM
    [FLINK-10687] [table] Move format descriptors and validators to flink-table-common Timo Walther* 2018/10/27, 9:47 PM
    [FLINK-10245] [Streaming Connector] Add Pojo, Tuple, Row and Scala Product DataStream Sink and Upsert Table Sink for HBase Shimin Yang 2018/8/28, 6:25 PM
    [hotfix] Fix checkstyle violations in SlotManager Till Rohrmann* 2018/9/18, 5:18 PM
    [hotfix] Fix checkstyle violations in SlotManager Till Rohrmann* 2018/9/18, 5:18 PM
    [FLINK-10260] Clean up log messages for TaskExecutor registrations Andrey Zagrebin* 2018/9/20, 8:57 PM
    [FLINK-9890][Distributed Coordination] Remove obsolete class ResourceManagerConfiguration gyao* 2018/7/18, 10:23 PM
    [FLINK-9455][RM] Add support for multi task slot TaskExecutors Till Rohrmann* 2018/8/22, 7:51 PM
    [hotfix] Cancel actual pending slot request in SlotManager#updateSlotState Till Rohrmann* 2018/9/21, 9:53 PM
    [hotfix] Remove mocking from SlotManagerTest Till Rohrmann* 2018/9/21, 11:16 PM
    [hotfix] Remove mocking from SlotProtocolTest Till Rohrmann* 2018/9/21, 11:24 PM
    [hotfix] Start MesosWorkers with default ContaineredTaskManagerConfiguration Till Rohrmann* 2018/9/22, 8:13 PM
    [FLINK-10099][test] Improve YarnResourceManagerTest 陈梓立* 2018/8/6, 4:09 PM
    [FLINK-10208][build] Bump mockito to 2.21.0 / powermock to 2.0.0-beta.5 Chesnay Schepler* 2018/10/8, 7:39 PM
    [FLINK-10848] Remove container requests after successful container allocation Till Rohrmann* 2019/1/8, 8:06 PM
    [FLINK-10726] [table] Include flink-table-common in flink-table jar Timo Walther 2018/10/30, 9:38 PM
