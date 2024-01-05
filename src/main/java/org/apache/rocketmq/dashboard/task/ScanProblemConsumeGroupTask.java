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
package org.apache.rocketmq.dashboard.task;

import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.raycloud.dmj.arms.api.ArmsSdkApi;
import com.raycloud.dmj.arms.api.IArmsSdkApi;
import com.raycloud.dmj.arms.api.domain.Notifier;
import com.raycloud.dmj.arms.api.enums.EventLevel;
import com.raycloud.dmj.arms.api.enums.EventSource;
import com.raycloud.dmj.arms.api.request.EventRequest;
import com.raycloud.dmj.arms.api.response.EventResponse;
import com.raycloud.dmj.arms.api.response.base.BaseResponse;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.dashboard.config.RMQConfigure;
import org.apache.rocketmq.dashboard.model.ClusterConfig;
import org.apache.rocketmq.dashboard.model.GroupConsumeInfo;
import org.apache.rocketmq.dashboard.model.QueueStatInfo;
import org.apache.rocketmq.dashboard.model.TopicConsumerInfo;
import org.apache.rocketmq.dashboard.service.ClusterService;
import org.apache.rocketmq.dashboard.service.ConsumerService;
import org.apache.rocketmq.dashboard.service.MonitorService;
import org.apache.rocketmq.dashboard.util.DingDingUtil;
import org.apache.rocketmq.dashboard.util.ProjectUtil;
import org.apache.rocketmq.dashboard.util.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.math.RoundingMode;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author zero
 */
@Component
@SuppressWarnings("SpellCheckingInspection")
public class ScanProblemConsumeGroupTask implements Runnable, InitializingBean, DisposableBean {

    private static final Logger LOGGER = LoggerFactory.getLogger("com.raycloud.dmj.scanProblemConsumeGroupTask");

    private final ScheduledExecutorService executorService = Executors.newSingleThreadScheduledExecutor();

    @Resource
    private RMQConfigure configure;

    @Resource
    private MonitorService monitorService;

    @Resource
    private ConsumerService consumerService;

    @Resource
    private ClusterService clusterService;

    private IArmsSdkApi armsSdkApi = new ArmsSdkApi();

    @Override
    public void run() {
        try {
            AlarmIndex alarmIndex = AlarmIndex.build(monitorService.queryConsumerMonitorConfig());
            List<GroupConsumeInfo> groupConsumeInfos = consumerService.queryGroupList(true);

            for (GroupConsumeInfo groupConsumeInfo : groupConsumeInfos) {
                if (-1 == groupConsumeInfo.getDiffTotal()) {
                    continue;
                }
                List<TopicConsumerInfo> topicConsumerInfos = Lists.newArrayList();
                Integer maxDiffTotal = alarmIndex.getMaxDiffTotalByGroup(groupConsumeInfo.getGroup());
                if (groupConsumeInfo.getDiffTotal() > maxDiffTotal) {
                    topicConsumerInfos = consumerService.queryConsumeStatsListByGroupName(groupConsumeInfo.getGroup());
                }
                if (CollectionUtils.isNotEmpty(topicConsumerInfos)) {
                    long diffTotal = 0L;
                    for (TopicConsumerInfo topicConsumerInfo : topicConsumerInfos) {
                        diffTotal += topicConsumerInfo.getDiffTotal();
                    }
                    groupConsumeInfo.setDiffTotal(diffTotal);
                }
                sendToDingTalk(groupConsumeInfo, alarmIndex, topicConsumerInfos);
            }
        } catch (Throwable throwable) {
            LOGGER.error(throwable.getMessage(), throwable);
        }

        try {
            ClusterConfig clusterConfig = monitorService.queryClusterConfig();
            Double putTps = clusterConfig.getPutTps();
            if (putTps == null) {
                return;
            }
            Map<String, Object> list = clusterService.list();
            Map<String/*brokerName*/, Map<Long/* brokerId */, Object/* brokerDetail */>> brokerServer = (Map) list.get("brokerServer");
            StringBuilder sb = new StringBuilder();
            brokerServer.forEach((brokerName, v) -> {
                v.forEach((brokerId, brokerDetail) -> {
                    Map<String, String> kvTable = (Map) brokerDetail;
                    String putTpsTemp = kvTable.get("putTps");
                    String[] putTpsArray = putTpsTemp.split(" ");
                    Double tps = Double.valueOf(putTpsArray[0]);
                    if (tps > putTps) {
                        sb.append(String.format("警告: 集群名【%s】brokerName【%s】brokerId【%s】生产消息TPS【%s】阈值【%s】\n\n", clusterConfig.getClusterName(), brokerName, brokerId, tps, putTps));
                    }
                });

            });
            if(StringUtils.isBlank(sb.toString())){
                return;
            }
            DingDingUtil.send(sb.toString(), clusterConfig.getDingdingHook(), clusterConfig.getAtMobiles());
        } catch (Throwable throwable) {
            LOGGER.error(throwable.getMessage(), throwable);
        }
    }

    /**
     * 发送钉钉告警
     *
     * @param consumeInfo 消费者组的消费信息
     * @param alarmIndex  消费组的告警阈值
     */
    public void sendToDingTalk(GroupConsumeInfo consumeInfo, AlarmIndex alarmIndex, List<TopicConsumerInfo> topicConsumerInfos) {
        String clusterName = ProjectUtil.getLocalName();
        ClusterConfig clusterConfig = monitorService.queryClusterConfig();
        if (clusterConfig != null && StringUtils.isNotBlank(clusterConfig.getClusterName())) {
            clusterName = clusterConfig.getClusterName();
        }
        List<String> telephonesByGroup = alarmIndex.getTelephonesByGroup(consumeInfo.getGroup());
        List<Notifier> notifiers = new ArrayList<>();
        for (String telephone : telephonesByGroup) {
            Notifier notifier = new Notifier();
            notifier.setPhoneNumber(telephone.trim());
            notifiers.add(notifier);
        }
        EventRequest eventRequest = buildEventRequest(clusterName);
        eventRequest.setTitle(consumeInfo.getGroup());
        eventRequest.setNotifiers(notifiers);
        eventRequest.setEventLevel(EventLevel.findByName(alarmIndex.getEventLevel(consumeInfo.getGroup())));
        eventRequest.setAlarmWebHook(alarmIndex.getHookByGroup(consumeInfo.getGroup()));

        Integer minCount = alarmIndex.getMinCountByGroup(consumeInfo.getGroup());
        if (consumeInfo.getCount() < minCount) {
            eventRequest.setEventName(consumeInfo.getGroup() + "消费者数量过低");
            String content = "警告: 集群名 【" + clusterName + "】 group【" + consumeInfo.getGroup() + "】" + "消费者数量过低 " + "当前消费者数量【" + consumeInfo.getCount() + "】" + "阀值【**" + minCount + "】";
            eventRequest.setContent(content);
            Boolean success = sendArms(eventRequest);
            if(!success){
                DingDingUtil.send(content, alarmIndex.getHookByGroup(consumeInfo.getGroup()), alarmIndex.getTelephonesByGroup(consumeInfo.getGroup()));
            }
        }

        Integer maxDiffTotal = alarmIndex.getMaxDiffTotalByGroup(consumeInfo.getGroup());
        if (consumeInfo.getDiffTotal() > maxDiffTotal) {
            String content = "警告: 集群名 【**" + clusterName + "**】 group【**" + consumeInfo.getGroup() + "**】" + "消息堆积过多 " + "当前消息堆积值【**" + consumeInfo.getDiffTotal() + "**】" + "阀值【**" + maxDiffTotal + "**】\n";
            Tuple2<String, Map<Integer, String>> tuple2 = getTopicConsumerInfos(topicConsumerInfos);
            content = content + tuple2.getT1();
            if (CollectionUtils.isNotEmpty(topicConsumerInfos) && clusterName.contains("ec")) {
                String topic = topicConsumerInfos.get(topicConsumerInfos.size() -1).getTopic();
                topic = topic.replace("%RETRY%", "");
                topic = topic.replace("_consumer_group", "");
                content = content + "\n [详细请看](https://erp-grafana.superboss.cc/d/3mQcH8iVk/xiao-xi-sheng-chan-xiao-fei-tong-ji?orgId=1&var-topic=" + topic + "&var-consumerGroup=" + consumeInfo.getGroup() + "&var-brokerAddr=All&var-queueId=All&var-eventName=All&var-hostName=All)";
            }
            eventRequest.setEventName(consumeInfo.getGroup() + "消息堆积过多");
            eventRequest.setContent(content);
            if(tuple2.getT2() != null){
                eventRequest.setExt(JSON.toJSONString(tuple2.getT2()));
            }
            Boolean success = sendArms(eventRequest);
            if(!success){
                DingDingUtil.sendMarkdown("MQ警告", content, alarmIndex.getHookByGroup(consumeInfo.getGroup()), alarmIndex.getTelephonesByGroup(consumeInfo.getGroup()));
            }
        }
    }

    private Boolean sendArms(EventRequest eventRequest) {
        try{
            if(StringUtils.isNotBlank(eventRequest.getAlarmWebHook())){
                BaseResponse<EventResponse> response = armsSdkApi.sendEvent(eventRequest);
                if(response.isSuccess()){
                    return true;
                }else{
                    LOGGER.error("发送到arms失败: " + response.getMessage());
                }
            }
            return false;
        }catch (Throwable e){
            LOGGER.error("发送到arms失败2", e);
            return false;
        }
    }
    private EventRequest buildEventRequest(String clusterName) {
        EventRequest eventRequest = new EventRequest();
        eventRequest.setAppKey("asfsgjdfjgnosdpas");
        if(clusterName.contains("日志")){
            eventRequest.setSource(EventSource.MQ_LOG_CONSOLE);
        }else if(clusterName.contains("灰度")){
            eventRequest.setSource(EventSource.MQ_GRAY_CONSOLE);
        }else{
            eventRequest.setSource(EventSource.MQ_EC_CONSOLE);
        }

        eventRequest.setCreateTime(new Date());

        return eventRequest;
    }


    /**
     * 获取每个客户端消费延迟详情
     */
    public static Tuple2<String, Map<Integer,String>> getTopicConsumerInfos(List<TopicConsumerInfo> topicConsumerInfos) {
        StringBuilder subContent = new StringBuilder();
        if (CollectionUtils.isEmpty(topicConsumerInfos)) {
            return new Tuple2<>(null, null);
        }
        long diffTotalSum = 0L;
        subContent.append("\n **客户端消费延迟详情**：\n");
        Map<String, Long> clientDiffTotal = Maps.newTreeMap();
        for (TopicConsumerInfo topicConsumerInfo : topicConsumerInfos) {
            diffTotalSum += topicConsumerInfo.getDiffTotal();
            List<QueueStatInfo> queueStatInfoList = topicConsumerInfo.getQueueStatInfoList();
            for (QueueStatInfo queueStatInfo : queueStatInfoList) {
                Long diffTotal = queueStatInfo.getBrokerOffset() - queueStatInfo.getConsumerOffset();
                String key = "- ###### broker: 【**" + queueStatInfo.getBrokerName() + "**】" + " queue: 【**" + queueStatInfo.getQueueId() + "**】" + " consumerClient: 【**" + queueStatInfo.getClientInfo() + "**】" + " lastTimestamp: 【**" + longToDate(queueStatInfo.getLastTimestamp()) + "**】";
                if (clientDiffTotal.containsKey(key)) {
                    clientDiffTotal.put(key, diffTotal + clientDiffTotal.get(key));
                } else {
                    clientDiffTotal.put(key, diffTotal);
                }
            }
        }
        List<Map.Entry<String, Long>> list = new ArrayList<Map.Entry<String, Long>>(clientDiffTotal.entrySet());
        list.sort(new Comparator<Map.Entry<String, Long>>() {
            @Override
            public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                return (int) (o2.getValue() - o1.getValue());
            }
        });
        Map<Integer,String> map = new TreeMap<>();
        int maxSize = Math.min(list.size(), 5);
        for (int i = 0; i < maxSize; i++) {
            Map.Entry<String, Long> stringLongEntry = list.get(i);
            Double percent = stringLongEntry.getValue() / (diffTotalSum * 1.00);
            String s = formatDouble2(percent);
            String subMsg = stringLongEntry.getKey() + " 堆积值：**" + stringLongEntry.getValue() + "** 百分比：**" + s + "**\n";
            subContent.append(subMsg);
            map.put(i, subMsg);
        }
        return new Tuple2<>(subContent.toString(), map);
    }


    /**
     * double 转String保留两位小数
     */
    public static String formatDouble2(Double d) {
        NumberFormat nf = NumberFormat.getNumberInstance();
        // 保留两位小数
        nf.setMaximumFractionDigits(2);
        // 如果不需要四舍五入，可以使用RoundingMode.DOWN
        nf.setRoundingMode(RoundingMode.UP);
        return nf.format(d);
    }

    public static String longToDate(long lo) {
        if (lo <= 0) {
            return "";
        }
        Date date = new Date(lo);
        SimpleDateFormat sd = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return sd.format(date);
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        executorService.scheduleAtFixedRate(this, configure.getMonitorPeriod(), configure.getMonitorPeriod(), TimeUnit.SECONDS);
    }

    @Override
    public void destroy() throws Exception {
        executorService.shutdown();
    }
}
