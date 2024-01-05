package org.apache.rocketmq.dashboard.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.collections.CollectionUtils;
import org.apache.rocketmq.dashboard.task.HealthCheckTask;
import org.apache.rocketmq.dashboard.task.dingding.At;
import org.apache.rocketmq.dashboard.task.dingding.DingDingMessage;
import org.apache.rocketmq.dashboard.task.dingding.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.*;
import org.springframework.http.client.SimpleClientHttpRequestFactory;
import org.springframework.web.client.RestTemplate;

import java.util.List;

/**
 * @author zero
 */
public class DingDingUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(HealthCheckTask.class);

    private static final RestTemplate REST_TEMPLATE = new RestTemplate(new SimpleClientHttpRequestFactory());

    public static void send(String content, String hook, List<String> atMobiles) {
        //新建Http头，add方法可以添加参数
        HttpHeaders headers = new HttpHeaders();
        //设置请求发送方式
        HttpMethod method = HttpMethod.POST;
        headers.setContentType(MediaType.APPLICATION_JSON_UTF8);
        DingDingMessage dingDingMessage = new DingDingMessage();
        Text text = new Text(content);
        dingDingMessage.setText(text);
        At at = new At();
        at.setAtMobiles(atMobiles);
        dingDingMessage.setAt(at);
        //将请求头部和参数合成一个请求
        HttpEntity<String> requestEntity = new HttpEntity<>(dingDingMessage.toString(), headers);
        //执行HTTP请求，将返回的结构使用String 类格式化（可设置为对应返回值格式的类）
        ResponseEntity<String> response = REST_TEMPLATE.exchange(hook, method, requestEntity, String.class);
        JSONObject resJson = JSON.parseObject(response.getBody());
        if (response.getStatusCode() == HttpStatus.OK && resJson.getIntValue("errcode") == 0) {
            return;
        }
        String errMsg = String.format("钉钉推送消息失败，错误码【%d】,错误信息【%s】", resJson.getIntValue("errcode"), resJson.getString("errmsg"));
        LOGGER.error(errMsg);
    }

    public static void sendMarkdown(String title,String content, String hook, List<String> atMobiles) {
        //新建Http头，add方法可以添加参数
        HttpHeaders headers = new HttpHeaders();
        //设置请求发送方式
        HttpMethod method = HttpMethod.POST;
        headers.setContentType(MediaType.APPLICATION_JSON_UTF8);
        JSONObject json = new JSONObject();
        json.put("msgtype", "markdown");
        JSONObject md = new JSONObject();
        if(CollectionUtils.isNotEmpty(atMobiles)){
            for (String atMobile : atMobiles) {
                content = content + "\n @" + atMobile + " ";
            }
        }
        md.put("text", content);
        md.put("title", title);
        json.put("markdown", md);
        json.put("at", "{\"atMobiles\":"+JSON.toJSONString(atMobiles)+"}");
        //将请求头部和参数合成一个请求
        HttpEntity<String> requestEntity = new HttpEntity<>(json.toString(), headers);
        //执行HTTP请求，将返回的结构使用String 类格式化（可设置为对应返回值格式的类）
        ResponseEntity<String> response = REST_TEMPLATE.exchange(hook, method, requestEntity, String.class);
        JSONObject resJson = JSON.parseObject(response.getBody());
        if (response.getStatusCode() == HttpStatus.OK && resJson.getIntValue("errcode") == 0) {
            return;
        }
        String errMsg = String.format("钉钉推送消息失败，错误码【%d】,错误信息【%s】", resJson.getIntValue("errcode"), resJson.getString("errmsg"));
        LOGGER.error(errMsg);
    }

}
