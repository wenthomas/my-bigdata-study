package com.wenthomas.flink.transform;

import com.wenthomas.flink.pojo.SampleEvent;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlatMapTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<SampleEvent> stream = env.fromElements(
                new SampleEvent("Mary", "./home", 1000L),
                new SampleEvent("Bob", "./cart", 2000L)
        );

        stream.flatMap(new MyFlatMap()).print();
        env.execute();
    }

    public static class MyFlatMap implements FlatMapFunction<SampleEvent, String> {
        @Override
        public void flatMap(SampleEvent value, Collector<String> out) throws Exception {
            if (value.user.equals("Mary")) {
                out.collect(value.user);
            } else if (value.user.equals("Bob")) {
                out.collect(value.user);
                out.collect(value.url);
            }
        }
    }
}
