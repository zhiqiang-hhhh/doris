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

def getMetrics = { ip, port ->
        def dst = 'http://' + ip + ':' + port
        def conn = new URL(dst + "/metrics").openConnection()
        conn.setRequestMethod("GET")
        def encoding = Base64.getEncoder().encodeToString((context.config.feHttpUser + ":" + 
                (context.config.feHttpPassword == null ? "" : context.config.feHttpPassword)).getBytes("UTF-8"))
        conn.setRequestProperty("Authorization", "Basic ${encoding}")
        return conn.getInputStream().getText()
    }

suite("test_metrics_with_workload_group") {
    for (int i = 0; i < 20; i++) {
        // 1. SHOW BACKENDS get be ip and http port
        Map<String, String> backendId_to_backendIP = new HashMap<>();
        Map<String, String> backendId_to_backendHttpPort = new HashMap<>();
        getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);
        // Print above maps in logger.
        logger.info("backendId_to_backendIP: " + backendId_to_backendIP);
        logger.info("backendId_to_backendHttpPort: " + backendId_to_backendHttpPort);

        // 2. CREATE WORKLOAD GROUP
        sql "drop workload group if exists test_wg_metrics;"
        sql "create workload group if not exists test_wg_metrics " +
                "properties ( " +
                "    'cpu_share'='10', " +
                "    'memory_limit'='10%', " +
                "    'enable_memory_overcommit'='true' " +
                ");"
        sql "set workload_group=test_wg_metrics;"
        wg = sql("select name,cpu_share,memory_limit,enable_memory_overcommit,max_concurrency,max_queue_size,queue_timeout,cpu_hard_limit,scan_thread_num,tag,read_bytes_per_second,remote_read_bytes_per_second from information_schema.workload_groups where name = 'test_wg_metrics' order by name;");
        logger.info("wg: " + wg);

        // 3. EXECUTE A QUERY SO THAT THE WORKLOAD GROUP IS USED
        sql "select count(*) from numbers(\"number\"=\"100\");"
        
        // curl backend http port to get metrics
        // get first backendId
        backendId = backendId_to_backendIP.keySet().iterator().next();
        backendIP = backendId_to_backendIP.get(backendId);
        backendHttpPort = backendId_to_backendHttpPort.get(backendId);
        logger.info("backendId: " + backendId + ", backendIP: " + backendIP + ", backendHttpPort: " + backendHttpPort);

        // Create a for loop to get metrics 5 times
        for (int j = 0; j < 5; j++) {
            String metrics = getMetrics(backendIP, backendHttpPort);
            String filteredMetrics = metrics.split('\n').findAll { line ->
                line.startsWith('doris_be_thread_pool') && line.contains('workload_group="test_wg_metrics"') && line.contains('thread_pool_name="Scan_test_wg_metrics"')
            }.join('\n')
            // Filter metrics with name test_wg_metrics
            logger.info("filteredMetrics: " + filteredMetrics);
            List<String> lines = filteredMetrics.split('\n').findAll { it.trim() }
            assert lines.size() == 5
        }

        sql "drop workload group if exists test_wg_metrics;"
    }
    sql "drop workload group if exists test_wg_metrics;"
}