package com.wenthomas.flink.transform;

import com.wenthomas.flink.pojo.SampleEvent;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<SampleEvent> stream = env.fromElements(
                new SampleEvent("Mary", "./home", 1000L),
                new SampleEvent("Bob", "./cart", 2000L)
        );

        //实现方式1:传入匿名类，实现MapFunction
        stream.map(new MapFunction<SampleEvent, String>() {
            @Override
            public String map(SampleEvent e) throws Exception {
                return e.user;
            }
        });

        //实现方式2:传入MapFunction的实现类
        stream.map(new UserExtractor()).print();
        env.execute();
    }

    public static class UserExtractor implements MapFunction<SampleEvent, String> {

        @Override
        public String map(SampleEvent sampleEvent) throws Exception {
            return sampleEvent.user;
        }
    }
}
