package org.apache.rocketmq.dashboard.task;

import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.SendMessageContext;
import org.apache.rocketmq.client.hook.SendMessageHook;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.protocol.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.dashboard.config.RMQConfigure;
import org.apache.rocketmq.dashboard.model.ConsumerMonitorConfig;
import org.apache.rocketmq.dashboard.model.request.ConsumerConfigInfo;
import org.apache.rocketmq.dashboard.model.request.TopicConfigInfo;
import org.apache.rocketmq.dashboard.service.ConsumerService;
import org.apache.rocketmq.dashboard.service.MonitorService;
import org.apache.rocketmq.dashboard.service.TopicService;
import org.apache.rocketmq.dashboard.util.DingDingUtil;
import org.apache.rocketmq.dashboard.util.ProjectUtil;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingConnectException;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.tools.admin.MQAdminExt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author zero
 */
@Component
public class HealthCheckTask implements Runnable, InitializingBean, DisposableBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(HealthCheckTask.class);

    public static final String HEALTH_CHECK_TOPIC = "HEALTH_CHECK_TOPIC";

    public static final String HEALTH_CHECK_PRODUCER_GROUP = "HEALTH_CHECK_PRODUCER_GROUP";

    public static final String HEALTH_CHECK_CONSUMER_GROUP = "HEALTH_CHECK_CONSUMER_GROUP";

    @Resource
    private RMQConfigure configure;

    @Resource
    private MonitorService monitorService;

    @Resource
    protected MQAdminExt mqAdminExt;

    @Resource
    private TopicService topicService;

    @Resource
    private ConsumerService consumerService;

    private DefaultMQProducer producer;

    private DefaultMQPushConsumer consumer;

    private ScheduledExecutorService healthCheckExecutorService = Executors.newSingleThreadScheduledExecutor();

    @Override
    public void run() {
        try {
            // 这里对每个队列都进行消息发送，保证对各个broker都雨露均沾
            AtomicInteger queueIndex = new AtomicInteger(0);
            AtomicBoolean running = new AtomicBoolean(true);

            AtomicReference<String> brokerName = new AtomicReference<>();
            while (running.get()) {
                Thread.sleep(100);
                try {
                    final long beginTimestamp = System.currentTimeMillis();
                    producer.send(buildMessage(), (list, message, o) -> {
                        int index = queueIndex.getAndIncrement();
                        if (index == list.size() - 1) {
                            running.getAndSet(false);
                        }
                        MessageQueue queue = list.get(index);
                        brokerName.set(queue.getBrokerName());
                        return queue;
                    }, null);
                    ProducerStats.incSendRequestSuccessCount(brokerName.get());
                    ProducerStats.addSendMessageSuccessTimeTotal(brokerName.get(), System.currentTimeMillis() - beginTimestamp);
                } catch (Throwable throwable) {
                    ProducerStats.incSendRequestFailedCount(brokerName.get());
                }
            }
        } catch (Throwable throwable) {
            LOGGER.error(throwable.getMessage(), throwable);
        } finally {
            try {
                ProducerStats.printStats();
                ProducerStats.report();
                ProducerStats.reset();
            } catch (Throwable throwable) {
                LOGGER.error(throwable.getMessage(), throwable);
            }
        }
    }

    private static Message buildMessage() throws UnsupportedEncodingException {
        Message msg = new Message();
        msg.setTopic(HEALTH_CHECK_TOPIC);
        msg.setBody("hello baby".getBytes(RemotingHelper.DEFAULT_CHARSET));
        return msg;
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        List<String> clusterNameList = getClusterNameList();
        createTopic(clusterNameList);
        createConsumerGroup(clusterNameList);

        initAndStartProducer();
        initAndStartConsumer();

        startScheduleTask();
    }

    private void initAndStartProducer() throws MQClientException {
        ProducerStats.setConfigure(configure);
        ProducerStats.setMonitorService(monitorService);
        producer = new DefaultMQProducer(HEALTH_CHECK_PRODUCER_GROUP);
        producer.getDefaultMQProducerImpl().registerSendMessageHook(new SendMessageHook() {
            @Override
            public String hookName() {
                return "fetchBrokerMapping";
            }

            @Override
            public void sendMessageBefore(SendMessageContext context) {
                ProducerStats.setBrokerMapping(context.getMq().getBrokerName(), context.getBrokerAddr());
            }

            @Override
            public void sendMessageAfter(SendMessageContext context) {

            }
        });
        producer.setNamesrvAddr(configure.getNamesrvAddr());
        producer.setInstanceName(String.valueOf(System.currentTimeMillis()));
        producer.start();
    }

    private void initAndStartConsumer() throws MQClientException {
        consumer = new DefaultMQPushConsumer(HEALTH_CHECK_CONSUMER_GROUP);
        consumer.setNamesrvAddr(configure.getNamesrvAddr());
        consumer.setInstanceName(String.valueOf(System.currentTimeMillis()));
        consumer.subscribe(HEALTH_CHECK_TOPIC, "*");
        consumer.registerMessageListener((MessageListenerConcurrently) (list, consumeConcurrentlyContext) -> ConsumeConcurrentlyStatus.CONSUME_SUCCESS);
        consumer.start();
    }

    private void startScheduleTask() {
        healthCheckExecutorService.scheduleAtFixedRate(this, configure.getHealthCheckPeriod(), configure.getHealthCheckPeriod(), TimeUnit.SECONDS);
    }

    private List<String> getClusterNameList() throws RemotingSendRequestException, RemotingConnectException, RemotingTimeoutException, MQBrokerException, InterruptedException {
        ClusterInfo clusterInfo = mqAdminExt.examineBrokerClusterInfo();
        Map<String, Set<String>> clusterAddrTable = clusterInfo.getClusterAddrTable();
        return Lists.newArrayList(clusterAddrTable.keySet());
    }

    private void createTopic(List<String> clusterNameList) {
        TopicConfigInfo topicConfigInfo = new TopicConfigInfo();
        topicConfigInfo.setClusterNameList(clusterNameList);
        topicConfigInfo.setTopicName(HEALTH_CHECK_TOPIC);
        topicConfigInfo.setWriteQueueNums(16);
        topicConfigInfo.setReadQueueNums(16);
        topicConfigInfo.setPerm(6);
        topicService.createOrUpdate(topicConfigInfo);
    }

    private void createConsumerGroup(List<String> clusterNameList) {
        ConsumerConfigInfo consumerConfigInfo = new ConsumerConfigInfo();
        consumerConfigInfo.setClusterNameList(clusterNameList);

        SubscriptionGroupConfig subscriptionGroupConfig = new SubscriptionGroupConfig();
        subscriptionGroupConfig.setGroupName(HEALTH_CHECK_CONSUMER_GROUP);
        consumerConfigInfo.setSubscriptionGroupConfig(subscriptionGroupConfig);
        consumerService.createAndUpdateSubscriptionGroupConfig(consumerConfigInfo);
    }

    @Override
    public void destroy() throws Exception {
        if (Objects.nonNull(producer)) {
            producer.shutdown();
        }
        if (Objects.nonNull(consumer)) {
            consumer.shutdown();
        }
        healthCheckExecutorService.shutdown();
    }

    static class ProducerStats {

        private static RMQConfigure configure;

        private static MonitorService monitorService;

        private static final Map<String, BrokerStats> brokerStatsMap = new HashMap<>();

        private static final Map<String, Boolean> lastReportMap = new HashMap<>();

        private static final Map<String, String> brokerMapping = new HashMap<>();

        public static void incSendRequestSuccessCount(String brokerName) {
            brokerStatsMap.computeIfAbsent(brokerName, key -> new BrokerStats(brokerName)).getSendRequestSuccessCount().incrementAndGet();
        }

        public static void incSendRequestFailedCount(String brokerName) {
            brokerStatsMap.computeIfAbsent(brokerName, key -> new BrokerStats(brokerName)).getSendRequestFailedCount().incrementAndGet();
        }

        public static void addSendMessageSuccessTimeTotal(String brokerName, long took) {
            brokerStatsMap.computeIfAbsent(brokerName, key -> new BrokerStats(brokerName)).getSendMessageSuccessTimeTotal().addAndGet(took);
        }

        public static void printStats() {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(ProjectUtil.getLocalName());
            stringBuilder.append(" health check detail, ");
            for (BrokerStats brokerStats : brokerStatsMap.values()) {
                stringBuilder.append("broker[").append(brokerMapping.get(brokerStats.getBrokerName())).append("]: ").append("sendRequestSuccessCount=").append(brokerStats.getSendRequestSuccessCount().get()).append(",").append("sendRequestFailedCount=").append(brokerStats.getSendRequestFailedCount().get()).append(",").append("sendMessageSuccessTimeTotal=").append(brokerStats.getSendMessageSuccessTimeTotal()).append("ms; ");
            }
            LOGGER.info(stringBuilder.toString());
        }

        public static void report() {
            for (BrokerStats brokerStats : brokerStatsMap.values()) {
                if (brokerStats.calSendMessageFailedRateAndRet() > configure.getHealthCheckFailureRate()) {
                    String content = "严重：控制台【" + ProjectUtil.getLocalName() + "】" + "健康检查broker【" + brokerMapping.get(brokerStats.getBrokerName()) + "】错误率过高，" + "当前错误率【" + brokerStats.calSendMessageFailedRateAndRet() + "%】，阈值【" + configure.getHealthCheckFailureRate() + "%】";
                    reportToDingDing(content);
                    lastReportMap.put(brokerStats.getBrokerName(), true);
                    continue;
                }
                if (brokerStats.calSendMessageSuccessAvgTookAndRet() > configure.getHealthCheckAvgTookThreshold()) {
                    String content = "严重：控制台【" + ProjectUtil.getLocalName() + "】" + "健康检查broker【" + brokerMapping.get(brokerStats.getBrokerName()) + "】平均took过高，" + "当前平均took值【" + brokerStats.calSendMessageSuccessAvgTookAndRet() + "ms】，阈值【" + configure.getHealthCheckAvgTookThreshold() + "ms】";
                    reportToDingDing(content);
                    lastReportMap.put(brokerStats.getBrokerName(), true);
                    continue;
                }
                // 如果上一次告警过，这一次恢复了，则需要发送恢复告警的钉钉通知
                Boolean lastReported = lastReportMap.get(brokerStats.getBrokerName());
                if (Objects.nonNull(lastReported) && lastReported) {
                    String content = "恢复：控制台【" + ProjectUtil.getLocalName() + "】" + "健康检查broker【" + brokerMapping.get(brokerStats.getBrokerName()) + "】恢复正常";
                    reportToDingDing(content);
                    lastReportMap.put(brokerStats.getBrokerName(), false);
                }
            }
        }

        private static String getHealthCheckDingDingHook() {
            ConsumerMonitorConfig consumerMonitorConfig = monitorService.queryConsumerMonitorConfigByGroupName(HEALTH_CHECK_CONSUMER_GROUP);
            if (StringUtils.isNotBlank(consumerMonitorConfig.getDingdingHook())) {
                return consumerMonitorConfig.getDingdingHook();
            }
            return configure.getHealthCheckDingDingHook();
        }

        private static List<String> getHealthCheckDingDingReceiver() {
            ConsumerMonitorConfig consumerMonitorConfig = monitorService.queryConsumerMonitorConfigByGroupName(HEALTH_CHECK_CONSUMER_GROUP);
            if (consumerMonitorConfig.getTelephones().size() != 0) {
                return consumerMonitorConfig.getTelephones();
            }
            return configure.getHealthCheckDingDingReceiver();
        }

        private static void reportToDingDing(String content) {
            DingDingUtil.send(content, getHealthCheckDingDingHook(), getHealthCheckDingDingReceiver());
        }

        public static void reset() {
            brokerStatsMap.clear();
        }

        public static void setConfigure(RMQConfigure configure) {
            ProducerStats.configure = configure;
        }

        public static void setMonitorService(MonitorService monitorService) {
            ProducerStats.monitorService = monitorService;
        }

        public static void setBrokerMapping(String brokerName, String brokerAddr) {
            ProducerStats.brokerMapping.put(brokerName, brokerAddr);
        }
    }

    static class BrokerStats {

        private String brokerName;

        private final AtomicInteger sendRequestSuccessCount = new AtomicInteger(0);

        private final AtomicInteger sendRequestFailedCount = new AtomicInteger(0);

        private final AtomicLong sendMessageSuccessTimeTotal = new AtomicLong(0L);

        public BrokerStats(String brokerName) {
            this.brokerName = brokerName;
        }

        public String getBrokerName() {
            return brokerName;
        }

        public AtomicInteger getSendRequestSuccessCount() {
            return sendRequestSuccessCount;
        }

        public AtomicInteger getSendRequestFailedCount() {
            return sendRequestFailedCount;
        }

        public AtomicLong getSendMessageSuccessTimeTotal() {
            return sendMessageSuccessTimeTotal;
        }

        public long calSendMessageSuccessAvgTookAndRet() {
            return this.sendMessageSuccessTimeTotal.get() / this.sendRequestSuccessCount.get();
        }

        public int calSendMessageFailedRateAndRet() {
            return this.sendRequestFailedCount.get() * 100 / (this.sendRequestSuccessCount.get() + this.sendRequestFailedCount.get());
        }
    }
}
