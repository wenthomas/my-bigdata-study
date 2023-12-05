package com.wenthomas.flink.source;

import com.wenthomas.flink.pojo.SampleEvent;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

public class HelloSource {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //创建集合
        ArrayList<SampleEvent> clicks = new ArrayList<>();
        clicks.add(new SampleEvent("Mary", "./home", 1000L));
        clicks.add(new SampleEvent("Bob", "./cart", 2000L));

        //DataStreamSource<SampleEvent> stream = env.fromCollection(clicks);
        DataStreamSource<SampleEvent> stream = env.addSource(new ClickSource());

        stream.print("CollectionSource1");

        env.execute();

    }
}
