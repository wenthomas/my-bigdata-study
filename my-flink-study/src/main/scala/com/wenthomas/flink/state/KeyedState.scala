package com.wenthomas.flink.state

import java.util

import com.wenthomas.flink.api.source.SensorReading
import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, MapState, MapStateDescriptor, ReducingState, ReducingStateDescriptor, ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
/**
 * @author Verno
 * @create 2020-04-21 11:46 
 */
object KeyedState {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        //val inputStream = env.readTextFile("E:\\project\\my-bigdata-study\\my-flink-study\\src\\main\\resources\\sensor.txt")
        val inputStream = env.socketTextStream("mydata01", 7777)
        val dataStream = inputStream
                .map({
                    data => {
                        val dataArray = data.split(", ")
                        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
                    }
                })

        //todo
        val keyedStream = dataStream.keyBy(_.id)
                .process(new KeyedProcessor)

        keyedStream.print("keyedStream")

        env.execute("KeyedState Demo")
    }

}


class KeyedProcessor extends KeyedProcessFunction[String, SensorReading, Int] {

    //1，值状态
    var myState: ValueState[Int] = _

    //2，列表状态
    var myListState: ListState[String] = _

    //3，映射状态
    var myMapState: MapState[String, Double] = _

    //4，聚合状态
    var myReducingState: ReducingState[SensorReading] = _

    //初始化键控状态
    override def open(parameters: Configuration): Unit = {
        myState = getRuntimeContext.getState(new ValueStateDescriptor[Int]("my-state", classOf[Int]))

        myListState = getRuntimeContext.getListState(new ListStateDescriptor[String]("my-list-state", classOf[String]))

        myMapState = getRuntimeContext.getMapState(new MapStateDescriptor[String, Double]("my-map-state", classOf[String], classOf[Double]))

        myReducingState = getRuntimeContext.getReducingState(new ReducingStateDescriptor[SensorReading](
            "my-reducing-state",
            new ReduceFunction[SensorReading] {
                override def reduce(value1: SensorReading, value2: SensorReading): SensorReading = {
                    //聚合函数
                    SensorReading(value1.id, value1.timestamp.max(value2.timestamp), value1.temperature.min(value2.temperature))
                }
            },
            classOf[SensorReading]))
    }

    //ProcessFunction函数内容
    //todo: 测试state api
    override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, Int]#Context, out: Collector[Int]): Unit = {
        myState.update(System.currentTimeMillis().toInt)
        myState.value()
        myListState.add("hello")
        myListState.update(new util.ArrayList[String]())
        myMapState.put("sensor_1", 10.0)
        myMapState.get("sensor_1")
        myReducingState.add(value)
        myReducingState.clear()
    }
}
