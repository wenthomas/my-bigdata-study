package com.wenthomas.flink.pojo;

import java.sql.Timestamp;

/**
 * 测试用Pojo
 */
public class SampleEvent {
    public String user;
    public String url;
    public Long timestamp;

    public SampleEvent(){

    }

    public SampleEvent(String user, String url, Long timestamp) {
        this.user = user;
        this.url = url;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "SampleEvent{" +
                "user='" + user + '\'' +
                ", url='" + url + '\'' +
                ", timestamp=" + new Timestamp(timestamp) +
                '}';
    }
}
