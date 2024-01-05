package org.apache.rocketmq.dashboard.util;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author admin
 */
public class ProjectUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProjectUtil.class);

    public static final String EMPTY = "";


    public static final String LOCAL_NAME;

    static {
        LOCAL_NAME = getLocalName();
    }

    public static String getLocalName() {
        try {
            String userHomePath = System.getProperty("user.home");
            if (StringUtils.isBlank(userHomePath)) {
                LOGGER.warn("can not find the system property for user.home");
                return EMPTY;
            }
            if (!userHomePath.contains("/")) {
                return EMPTY;
            }
            String[] userHomePathSplit = userHomePath.split("/");
            return userHomePathSplit[userHomePathSplit.length - 1];
        } catch (Exception e) {
            LOGGER.error("get localName fail, errMsg: {}", e.getMessage(), e);
        }
        return EMPTY;
    }
}
