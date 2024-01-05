package org.apache.rocketmq.dashboard.task;

import com.raycloud.dmj.arms.api.enums.EventLevel;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.rocketmq.dashboard.model.ConsumerMonitorConfig;
import org.apache.rocketmq.dashboard.task.dingding.Person;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

/**
 * @author zero
 */
public class AlarmIndex {

    private static Logger logger = LoggerFactory.getLogger(AlarmIndex.class);

    public static String DEFAULT_HOOK = "https://oapi.dingtalk.com/robot/send?access_token=fc6ab9b4a50c3950d9f8acb2b2b3a3185bfa29e8f218a14fa92339e94bf5233f";

    private static final int DEFAULT_MIN_COUNT = Integer.MIN_VALUE;

    private static final int DEFAULT_MAX_DIFF_TOTAL = 5000;

    public static List<String> DEFAULT_TELEPHONES = new ArrayList<>();

    private Map<String, CustomAlarmIndex> customGroupAlerts = new HashMap<>();

    private Set<String> excludeGroups = new HashSet<>();

    public static final DateTimeFormatter LOCAL_TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm");

    static {
        for (Person person : Person.values()) {
            DEFAULT_TELEPHONES.add(person.getTelephone());
        }
    }

    public static AlarmIndex build(Map<String, ConsumerMonitorConfig> consumerMonitorConfigMap) {
        AlarmIndex alarmIndex = new AlarmIndex();
        if (consumerMonitorConfigMap != null && consumerMonitorConfigMap.size() != 0) {
            for (Map.Entry<String, ConsumerMonitorConfig> consumerMonitorConfigEntry : consumerMonitorConfigMap.entrySet()) {
                CustomAlarmIndex customAlarmIndex = alarmIndex.getCustomGroupAlerts().get(consumerMonitorConfigEntry.getKey());
                if (Objects.isNull(customAlarmIndex)) {
                    customAlarmIndex = new CustomAlarmIndex();
                    customAlarmIndex.setGroup(consumerMonitorConfigEntry.getKey());
                    alarmIndex.getCustomGroupAlerts().put(consumerMonitorConfigEntry.getKey(), customAlarmIndex);
                }
                customAlarmIndex.setMinCount(consumerMonitorConfigEntry.getValue().getMinCount());
                String maxDiffTotal = consumerMonitorConfigEntry.getValue().getMaxDiffTotal();
                Integer max = 5000;
                try {
                    max = calculateMaxDiffTotal(maxDiffTotal);
                } catch (Exception e) {
                    logger.error("calculateMaxDiffTotal error", e);
                }
                customAlarmIndex.setMaxDiffTotal(max);

                customAlarmIndex.setTelephones(consumerMonitorConfigEntry.getValue().getTelephones());
                customAlarmIndex.setHook(consumerMonitorConfigEntry.getValue().getDingdingHook());
                customAlarmIndex.setEventLevel(consumerMonitorConfigEntry.getValue().getEventLevel());
            }
        }
        return alarmIndex;
    }

    public static Integer calculateMaxDiffTotal(String maxDiffTotal) {
        Integer res = 5000;
        LocalDateTime now = LocalDateTime.now();
        LocalTime nowLocalTime = now.toLocalTime();
        if (StringUtils.isNotBlank(maxDiffTotal) && maxDiffTotal.contains(",")) {
            String[] split = maxDiffTotal.split(",");
            for (String s : split) {
                if (s.contains("@") && s.split("@").length == 2) {
                    String[] split1 = s.split("@");
                    String[] split2 = split1[0].split("-");
                    LocalTime start = LocalTime.parse(split2[0], LOCAL_TIME_FORMATTER);
                    LocalTime end = LocalTime.parse(split2[1], LOCAL_TIME_FORMATTER);
                    if (nowLocalTime.isAfter(start) && nowLocalTime.isBefore(end)) {
                        res = StringUtils.isNotBlank(split1[1]) ? Integer.parseInt(split1[1]) : 5000;
                        break;
                    }
                } else {
                    res = StringUtils.isNotBlank(s) ? Integer.parseInt(s) : 5000;
                }
            }
        } else {
            res = StringUtils.isNotBlank(maxDiffTotal) ? Integer.parseInt(maxDiffTotal) : 5000;
        }

        return res;
    }


    public static class CustomAlarmIndex {

        private Integer minCount;

        private Integer maxDiffTotal;

        private String group;

        private String hook;

        private List<String> telephones;

        private String eventLevel;

        public Integer getMinCount() {
            return minCount;
        }

        public void setMinCount(Integer minCount) {
            this.minCount = minCount;
        }

        public Integer getMaxDiffTotal() {
            return maxDiffTotal;
        }

        public void setMaxDiffTotal(Integer maxDiffTotal) {
            this.maxDiffTotal = maxDiffTotal;
        }

        public String getGroup() {
            return group;
        }

        public void setGroup(String group) {
            this.group = group;
        }

        public String getHook() {
            return hook;
        }

        public void setHook(String hook) {
            this.hook = hook;
        }

        public List<String> getTelephones() {
            return telephones;
        }

        public void setTelephones(List<String> telephones) {
            this.telephones = telephones;
        }

        public String getEventLevel() {
            return eventLevel;
        }

        public void setEventLevel(String eventLevel) {
            this.eventLevel = eventLevel;
        }

        @Override
        public String toString() {
            return "CustomAlarmIndex{" + "minCount=" + minCount + ", maxDiffTotal=" + maxDiffTotal + ", group='" + group + '\'' + ", hook='" + hook + '\'' + ", telephones=" + telephones + '}';
        }
    }

    public Integer getMinCountByGroup(String group) {
        if (getExcludeGroups().contains(group)) {
            return Integer.MIN_VALUE;
        }
        if (getCustomGroupAlerts().containsKey(group)) {
            CustomAlarmIndex customAlarmIndex = getCustomGroupAlerts().get(group);
            return null == customAlarmIndex.getMinCount() ? DEFAULT_MIN_COUNT : customAlarmIndex.getMinCount();
        }
        return DEFAULT_MIN_COUNT;
    }

    public Integer getMaxDiffTotalByGroup(String group) {
        if (getExcludeGroups().contains(group)) {
            return Integer.MAX_VALUE;
        }
        if (getCustomGroupAlerts().containsKey(group)) {
            CustomAlarmIndex customAlarmIndex = getCustomGroupAlerts().get(group);
            return null == customAlarmIndex.getMaxDiffTotal() ? DEFAULT_MAX_DIFF_TOTAL : customAlarmIndex.getMaxDiffTotal();
        }
        return DEFAULT_MAX_DIFF_TOTAL;
    }

    public String getHookByGroup(String group) {
        if (StringUtils.isBlank(group) || !getCustomGroupAlerts().containsKey(group) || StringUtils.isBlank(getCustomGroupAlerts().get(group).getHook())) {
            return DEFAULT_HOOK;
        }

        return getCustomGroupAlerts().get(group).getHook();
    }

    public List<String> getTelephonesByGroup(String group) {
        if (StringUtils.isBlank(group) || !getCustomGroupAlerts().containsKey(group) || CollectionUtils.isEmpty(getCustomGroupAlerts().get(group).getTelephones())) {
            return DEFAULT_TELEPHONES;
        }
        return getCustomGroupAlerts().get(group).getTelephones();
    }

    public Map<String, CustomAlarmIndex> getCustomGroupAlerts() {
        return customGroupAlerts;
    }

    public void setCustomGroupAlerts(Map<String, CustomAlarmIndex> customGroupAlerts) {
        this.customGroupAlerts = customGroupAlerts;
    }

    public Set<String> getExcludeGroups() {
        return excludeGroups;
    }

    public void setExcludeGroups(Set<String> excludeGroups) {
        this.excludeGroups = excludeGroups;
    }

    public String getEventLevel(String group) {
        if (StringUtils.isBlank(group) || !getCustomGroupAlerts().containsKey(group) || StringUtils.isBlank(getCustomGroupAlerts().get(group).getEventLevel())) {
            return EventLevel.DEFAULT.getName();
        }
        return getCustomGroupAlerts().get(group).getEventLevel();
    }


    @Override
    public String toString() {
        return "AlarmIndex{" + "customGroupAlerts=" + customGroupAlerts + ", excludeGroups=" + excludeGroups + '}';
    }
}
