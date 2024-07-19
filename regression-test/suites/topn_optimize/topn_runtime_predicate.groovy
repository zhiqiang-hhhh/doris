// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("topn_runtime_predicate") {
    def tableName = "topn_runtime_predicate"
    sql "DROP TABLE IF EXISTS ${tableName}"
    sql "CREATE  TABLE if NOT EXISTS ${tableName} (c1 INT) DUPLICATE KEY(c1) DISTRIBUTED BY HASH (c1) BUCKETS 1 PROPERTIES ('replication_num' = '1');"
    sql """insert into ${tableName} values  (1),
                                                                                        (0),
                                                                                        (0)"""
    qt_select " select * from (select cast(c1 as int) t1 from ${tableName}) t  order by t1"
    qt_select " select * from (select cast(c1 as int) t1 from ${tableName}) t  order by t1 limit 1"
    sql "DROP TABLE IF EXISTS ${tableName}"
}