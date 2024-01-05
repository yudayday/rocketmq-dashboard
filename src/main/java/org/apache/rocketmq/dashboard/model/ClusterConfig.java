package org.apache.rocketmq.dashboard.model;

import java.io.Serializable;
import java.util.List;

/**
 * @author shipeng
 * @date 2022/10/10 下午8:43
 */

public class ClusterConfig implements Serializable {

    private String clusterName;

    private List<String> atMobiles;

    private String dingdingHook;

    private Double putTps;

    public Double getPutTps() {
        return putTps;
    }

    public void setPutTps(Double putTps) {
        this.putTps = putTps;
    }

    public String getClusterName() {
        return clusterName;
    }

    public void setClusterName(String clusterName) {
        this.clusterName = clusterName;
    }

    public List<String> getAtMobiles() {
        return atMobiles;
    }

    public void setAtMobiles(List<String> atMobiles) {
        this.atMobiles = atMobiles;
    }

    public String getDingdingHook() {
        return dingdingHook;
    }

    public void setDingdingHook(String dingdingHook) {
        this.dingdingHook = dingdingHook;
    }
}
