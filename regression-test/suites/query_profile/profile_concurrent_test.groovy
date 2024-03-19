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

import groovy.json.JsonOutput
import groovy.json.JsonSlurper

/**
*   @Params url is "/xxx"
*   @Return response body
*/
def http_get(url) {
    def dst = 'http://' + context.config.feHttpAddress
    def conn = new URL(dst + url).openConnection()
    conn.setRequestMethod("GET")
    //token for root
    conn.setRequestProperty("Authorization","Basic cm9vdDo=")
    return conn.getInputStream().getText()
}

def SUCCESS_MSG = 'success'
def SUCCESS_CODE = 0
def QUERY_NUM = 5

random = new Random()

def getRandomNumber(int num){
    return random.nextInt(num)
}

suite('profile_concurrent_test') {

}