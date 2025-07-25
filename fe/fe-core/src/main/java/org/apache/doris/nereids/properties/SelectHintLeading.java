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

package org.apache.doris.nereids.properties;

import org.apache.doris.nereids.hint.DistributeHint;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * select hint.
 * e.g. set_var(query_timeout='1800', exec_mem_limit='2147483648')
 */
public class SelectHintLeading extends SelectHint {
    // e.g. query_timeout='1800', exec_mem_limit='2147483648'
    private final List<String> parameters;
    private final Map<String, DistributeHint> strToHint;

    public SelectHintLeading(String hintName, List<String> parameters, Map<String, DistributeHint> strToHint) {
        super(hintName);
        this.parameters = parameters;
        this.strToHint = strToHint;
    }

    public List<String> getParameters() {
        return parameters;
    }

    public Map<String, DistributeHint> getStrToHint() {
        return strToHint;
    }

    @Override
    public String toString() {
        List<String> newParameters = new ArrayList<>();
        for (String param : parameters) {
            if (param.startsWith("shuffle")) {
                newParameters.add("shuffle");
            } else if (param.startsWith("broadcast")) {
                newParameters.add("broadcast");
            } else {
                newParameters.add(param);
            }
        }
        String leadingString = newParameters
                .stream()
                .collect(Collectors.joining(" "));
        return super.getHintName() + "(" + leadingString + ")";
    }
}
