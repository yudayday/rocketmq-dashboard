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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package org.apache.rocketmq.dashboard.service.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Throwables;
import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.annotation.PostConstruct;
import javax.annotation.Resource;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.dashboard.config.RMQConfigure;
import org.apache.rocketmq.dashboard.model.ClusterConfig;
import org.apache.rocketmq.dashboard.model.ConsumerMonitorConfig;
import org.apache.rocketmq.dashboard.service.MonitorService;
import org.apache.rocketmq.dashboard.task.AlarmIndex;
import org.apache.rocketmq.dashboard.util.JsonUtil;
import org.springframework.stereotype.Service;

@Service
public class MonitorServiceImpl implements MonitorService {


    @Resource
    private RMQConfigure rmqConfigure;

    private Map<String, ConsumerMonitorConfig> configMap = new ConcurrentHashMap<>();

    private ClusterConfig clusterConfig = new ClusterConfig();

    @Override
    public boolean createOrUpdateConsumerMonitor(String name, ConsumerMonitorConfig config) {
        configMap.put(name, config);// todo if write map success but write file fail
        writeToFile(getConsumerMonitorConfigDataPath(), configMap);
        return true;
    }

    @Override
    public boolean createOrUpdateClusterConfig(ClusterConfig config) {
        clusterConfig = config;
        fillClusterConfig(clusterConfig);
        writeToFile(getClusterConfigDataPath(), clusterConfig);
        return true;
    }

    @Override
    public ClusterConfig queryClusterConfig() {
        return clusterConfig;
    }

    @Override
    public Map<String, ConsumerMonitorConfig> queryConsumerMonitorConfig() {
        return configMap;
    }

    @Override
    public ConsumerMonitorConfig queryConsumerMonitorConfigByGroupName(String consumeGroupName) {
        return configMap.get(consumeGroupName);
    }

    @Override
    public boolean deleteConsumerMonitor(String consumeGroupName) {
        configMap.remove(consumeGroupName);
        writeToFile(getConsumerMonitorConfigDataPath(), configMap);
        return true;
    }

    //rocketmq.console.data.path/monitor/consumerMonitorConfig.json
    private String getConsumerMonitorConfigDataPath() {
        return rmqConfigure.getRocketMqDashboardDataPath() + File.separatorChar + "monitor" + File.separatorChar + "consumerMonitorConfig.json";
    }

    private String getClusterConfigDataPath() {
        return rmqConfigure.getRocketMqDashboardDataPath() + File.separatorChar + "monitor" + File.separatorChar + "clusterConfig.json";
    }

    private String getConsumerMonitorConfigDataPathBackUp() {
        return getConsumerMonitorConfigDataPath() + ".bak";
    }

    private void writeToFile(String path, Object data) {
        writeDataJsonToFile(path, JsonUtil.obj2String(data));
    }

    private void writeDataJsonToFile(String path, String dataStr) {
        try {
            MixAll.string2File(dataStr, path);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @PostConstruct
    private void loadData() throws IOException {
        System.out.println("======" + getConsumerMonitorConfigDataPath());
        String content = MixAll.file2String(getConsumerMonitorConfigDataPath());
        if (content == null) {
            content = MixAll.file2String(getConsumerMonitorConfigDataPathBackUp());
        }
        if (content == null) {
            return;
        }
        configMap = JsonUtil.string2Obj(content, new TypeReference<ConcurrentHashMap<String, ConsumerMonitorConfig>>() {
        });

        System.out.println("======" + getClusterConfigDataPath());
        String clusterConfigStr =  MixAll.file2String(getClusterConfigDataPath());
        if (clusterConfigStr == null) {
            return;
        }
        clusterConfig = JsonUtil.string2Obj(clusterConfigStr, new TypeReference<ClusterConfig>() {
        });
        fillClusterConfig(clusterConfig);
    }

    void fillClusterConfig(ClusterConfig config){
        if(StringUtils.isNotBlank(config.getDingdingHook())){
            AlarmIndex.DEFAULT_HOOK = config.getDingdingHook();
        }
        if(CollectionUtils.isNotEmpty(config.getAtMobiles())){
            AlarmIndex.DEFAULT_TELEPHONES = config.getAtMobiles();
        }
    }
}
