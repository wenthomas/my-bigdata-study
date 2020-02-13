package com.wenthomas.flume.mydata;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author Verno
 * @create 2020-02-13 11:35
 */
public class MyLogInterceptor implements Interceptor {

    private static Logger logger= LoggerFactory.getLogger(MyLogInterceptor.class);

    //创建一个放置复合要求数据的集合
    private List<Event> results=new ArrayList<>();

    private String startFlag="\"en\":\"start\"";

    @Override
    public void initialize() {

    }

    /**
     * 核心方法，拦截Event
     * @param event
     * @return
     */
    @Override
    public Event intercept(Event event) {
        byte[] body = event.getBody();
        //1，在header中添加key
        Map<String, String> headers = event.getHeaders();

        String bodyStr = new String(body, Charset.forName("utf-8"));

        boolean flag = true;

        if (bodyStr.contains(startFlag)) {
            //符合启动日志特征
            headers.put("topic", "topic_start");
            flag = ETLUtil.validStartLog(bodyStr);
        } else {
            //符合事件日志特征
            headers.put("topic", "topic_event");
            flag = ETLUtil.validEventLog(bodyStr);
        }

        //2，如果验证结果是false，拦截
        if (!flag) {
            return null;
        }

        return event;
    }

    @Override
    public List<Event> intercept(List<Event> list) {
        //1，先清空results
        results.clear();

        for (Event event : list) {
            Event result = intercept(event);

            //如果event不合法被interceptor()拦截，返回null
            if (null != result) {
                //合法的result加入集合中
                results.add(result);
            }
        }

        return results;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        /**
         * 创建一个拦截器对象
         * @return
         */
        @Override
        public Interceptor build() {
            return new MyLogInterceptor();
        }

        /**
         * 从flume的配置文件中读取参数
         * @param context
         */
        @Override
        public void configure(Context context) {

        }
    }
}
