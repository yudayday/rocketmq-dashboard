package org.apache.rocketmq.dashboard.task.dingding;

import com.alibaba.fastjson.JSON;

/**
 * @author zero
 */
public class DingDingMessage {

    private At at;

    private Text text;

    private String msgtype = "text";

    public DingDingMessage(At at, Text text, String msgtype) {
        this.at = at;
        this.text = text;
        this.msgtype = msgtype;
    }

    public DingDingMessage() {

    }

    public At getAt() {
        return at;
    }

    public void setAt(At at) {
        this.at = at;
    }

    public Text getText() {
        return text;
    }

    public void setText(Text text) {
        this.text = text;
    }

    public String getMsgtype() {
        return msgtype;
    }

    public void setMsgtype(String msgtype) {
        this.msgtype = msgtype;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
