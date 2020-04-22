package com.wenthomas.flink.state

import com.wenthomas.flink.api.source.SensorReading
import org.apache.flink.api.common.functions.{MapFunction, RichFlatMapFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._
/**
 * @author Verno
 * @create 2020-04-21 15:11 
 */
object StatePractice {
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

        //--------------------------------------------------------------------------------------------------------------
        //通过自定义RichFlatMapFunction实现flatMap算子带状态，类似可以自定义实现RichMapFunction等函数
        //注意：用到keyed State时需要输入流为KeyStream
        val warningStream = dataStream.keyBy("id")
                .flatMap(new TempChangeWarningWithFlatMap(10.0))

        //官方提供flatMapWithState算子，等价于实现上行自定义RichFlatMapFunction的功能
        val warningStream2 = dataStream.keyBy("id")
                .flatMapWithState[(String, Double, Double), Double]({
                    //参数0为输入数据，参数1为状态.
                    //输出时，参数0为输出数据，参数1为状态更新
                    //(1) 输入无状态情况
                    case (inputData: SensorReading, None) => (List.empty, Some(inputData.temperature))
                    //(2) 输入有状态的情况
                    case (inputData: SensorReading, lastTemp: Some[Double]) =>
                        val diff = (inputData.temperature - lastTemp.get).abs
                        if (diff > 10.0) {
                            (List((inputData.id, lastTemp.get, inputData.temperature)), Some(inputData.temperature))
                        } else {
                            (List.empty, Some(inputData.temperature))
                        }
                })

        warningStream.print("warningStream")
        warningStream2.print("warningStream2")

        env.execute("State Programing Demo ")
    }

}


/**
 * 自定义RichMapFunction：实现连续变温大于threshold报警
 * @param threshold 温度差阈值
 */
class TempChangeWarning(threshold: Double) extends RichMapFunction[SensorReading, (String, Double, Double)] {

    //定义状态变量：上一次的温度值
    private var lastTempState: ValueState[Double] = _

    //初始化状态变量
    override def open(parameters: Configuration): Unit = {
        lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp", classOf[Double]))
    }

    override def map(value: SensorReading): (String, Double, Double) = {
        //首先从状态中取出上一次的温度值
        val lastTemp = lastTempState.value()

        //更新状态
        lastTempState.update(value.temperature)

        //跟当前温度值计算差值，然后跟阈值比较，如果超过阈值则报警
        val diff = (value.temperature - lastTemp).abs
        if (diff > threshold) {
            (value.id, lastTemp, value.temperature)
        } else {
            //赋予初值，否则返回null会空指针异常
            (value.id, 0.0, 0.0)
        }
    }
}

/**
 * 自定义RichFlatMapFunction：实现连续变温大于threshold报警，但相比自定义RichMapFunction可以输出多个结果
 * @param threshold
 */
class TempChangeWarningWithFlatMap(threshold: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {

    //定义状态变量：上一次的温度值
    private var lastTempState: ValueState[Double] = _

    //初始化状态变量
    override def open(parameters: Configuration): Unit = {
        lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp-flatmap", classOf[Double]))
    }

    override def flatMap(value: SensorReading, out: Collector[(String, Double, Double)]): Unit = {
        //首先从状态中取出上一次的温度值
        val lastTemp = lastTempState.value()

        //更新状态
        lastTempState.update(value.temperature)

        //跟当前温度值计算差值，然后跟阈值比较，如果超过阈值则报警
        val diff = (value.temperature - lastTemp).abs
        if (diff > threshold) {
            out.collect((value.id, lastTemp, value.temperature))
        }
    }
}