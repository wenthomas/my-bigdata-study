package com.wenthomas.flume.interceptor;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

import java.util.List;

/**
 * @author Verno
 * @create 2020-01-14 20:38
 */
public class MyInterceptor implements Interceptor {
    public void initialize() {

    }

    /**
     * 拦截方法
     * @param event
     * @return
     */
    public Event intercept(Event event) {
        byte[] body = event.getBody();
        //根据body的内容设置header：根据首字母类型分类
        if ((body[0] < 'z') && (body[0] > 'a')) {
            event.getHeaders().put("type","letter");
        } else if ((body[0] < '9') && (body[0] > '0')) {
            event.getHeaders().put("type", "number");
        }
        return event;
    }

    public List<Event> intercept(List<Event> list) {
        for (Event event : list) {
            intercept(event);
        }
        return list;
    }

    public void close() {

    }

    /**
     * Builder
     */
    public static class MyBuilder implements Interceptor.Builder {

        public Interceptor build() {
            return new MyInterceptor();
        }

        public void configure(Context context) {

        }
    }
}
