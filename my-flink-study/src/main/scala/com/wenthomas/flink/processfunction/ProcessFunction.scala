package com.wenthomas.flink.processfunction

import com.wenthomas.flink.api.source.SensorReading
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector
/**
 * @author Verno
 * @create 2020-04-20 15:41 
 */
/**
 * 底层API
 */
object ProcessFunction {
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


        //通过底层API定义自定义ProcessFunction来实现官方API所不能实现的功能：监测连续升温报警
        val warningStream = dataStream
                .keyBy("id")
                .process(new TempIncredWarning(10 * 1000L))

        warningStream.print("warningStream")

        env.execute("ProcessFunction Demo")
    }
}

/**
 * 自定义ProcessFunction: 数据流为KeyStream的话需要继承KeyedProcessFunction
 * 注释：
 * 1，当输入温度对比上一个温度值高时：即为连续升温事件，更新时间状态，并注册定时器10s后触发报警
 * 2，当输入温度对比上一个温度值低时，或者为第一个数据：删除定时器并清空时间状态
 * @param interval
 */
class TempIncredWarning(interval: Long) extends KeyedProcessFunction[Tuple, SensorReading, String] {

    //由于需要跟之前的温度值对比，所以将上一个温度保存为状态
    //注意：需要加上lazy懒加载，否则第一次运行无法从上下文获取状态
    lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))

    //为了方便删除定时器，还需要保存定时器的时间戳
    lazy val curTimerTsState = getRuntimeContext.getState(new ValueStateDescriptor[Long]("cur-Timer-Ts", classOf[Long]))


    /**
     * 流中的每一个元素都会调用这个方法，调用结果将会放在Collector数据类型中输出。
     * @param value
     * @param ctx
     * @param out
     */
    override def processElement(value: SensorReading, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#Context, out: Collector[String]): Unit = {
        //首先取出状态
        val lastTemp = lastTempState.value()
        val curTimerTs = curTimerTsState.value()

        //将上次温度值的状态更新为当前数据的温度值
        lastTempState.update(value.temperature)

        //判断当前温度值，如果比之前温度高，且没有定时器的话，注册10s后的定时器
        if (value.temperature > lastTemp && curTimerTs == 0) {
            //设定定时触发时间
            val ts = ctx.timerService().currentProcessingTime() + interval
            //注册定时器（系统内部时间）
            ctx.timerService().registerProcessingTimeTimer(ts)
            curTimerTsState.update(ts)
        } else if (value.temperature < lastTemp) {
            //如果温度下降，则删除定时器
            ctx.timerService().deleteProcessingTimeTimer(curTimerTs)
            //清空状态
            curTimerTsState.clear()
        }
    }

    /**
     * 定时器触发，说明10s内没有温度下降，报警
     * @param timestamp
     * @param ctx
     * @param out
     */
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
        out.collect( "温度值连续" + interval/1000 + "秒上升" )
        //清空状态
        curTimerTsState.clear()
    }
}
