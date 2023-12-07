package com.wenthomas.flink.wordcount;

import com.wenthomas.flink.pojo.SampleEvent;
import com.wenthomas.flink.source.ClickSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class WordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<SampleEvent> stream = env.addSource(new ClickSource());
        SingleOutputStreamOperator<Tuple2<String, Long>> result = stream
                .filter(new FilterFunction<SampleEvent>() {
                    @Override
                    public boolean filter(SampleEvent sampleEvent) throws Exception {
                        if (null != sampleEvent) {
                            return true;
                        } else {
                            return false;
                        }
                    }
                })
                .map(event -> Tuple2.of(event.user, 1L))
                .returns(Types.TUPLE(Types.STRING, Types.LONG))
                .keyBy(t -> t.f0)
                .sum(1);
        result.print();
        env.execute();
    }

    public static class MyFlatMap implements FlatMapFunction<SampleEvent, String> {

        @Override
        public void flatMap(SampleEvent sampleEvent, Collector<String> collector) throws Exception {
            collector.collect(sampleEvent.user);
        }
    }
}
