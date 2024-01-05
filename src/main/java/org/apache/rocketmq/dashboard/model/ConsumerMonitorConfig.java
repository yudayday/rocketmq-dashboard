/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.dashboard.model;

import java.util.List;

public class ConsumerMonitorConfig {
    private int minCount;
    private String maxDiffTotal;
    private List<String> telephones;
    private String dingdingHook;
    private String eventLevel;

    public ConsumerMonitorConfig() {
    }

    public ConsumerMonitorConfig(int minCount, String maxDiffTotal, List<String> telephones, String dingdingHook, String eventLevel) {
        this.minCount = minCount;
        this.maxDiffTotal = maxDiffTotal;
        this.telephones = telephones;
        this.dingdingHook = dingdingHook;
        this.eventLevel = eventLevel;
    }

    public int getMinCount() {
        return minCount;
    }

    public void setMinCount(int minCount) {
        this.minCount = minCount;
    }

    public String getMaxDiffTotal() {
        return maxDiffTotal;
    }

    public void setMaxDiffTotal(String maxDiffTotal) {
        this.maxDiffTotal = maxDiffTotal;
    }

    public List<String> getTelephones() {
        return telephones;
    }

    public void setTelephones(List<String> telephones) {
        this.telephones = telephones;
    }

    public String getDingdingHook() {
        return dingdingHook;
    }

    public void setDingdingHook(String dingdingHook) {
        this.dingdingHook = dingdingHook;
    }

    public String getEventLevel() {
        return eventLevel;
    }

    public void setEventLevel(String eventLevel) {
        this.eventLevel = eventLevel;
    }
}
