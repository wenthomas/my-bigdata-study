package com.wenthomas.flink.state
import java.util

import com.wenthomas.flink.api.source.SensorReading
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
/**
 * @author Verno
 * @create 2020-04-24 4:05 
 */
/**
 * 算子状态
 */
object OperatorState {
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

        //调用自定义Mapper：自己实现算子状态的使用，算子状态不需keyBy为前提，而键控状态需要keyBy为前提
        val opStateStream = dataStream.map(new MyMapper)

        opStateStream.print()

        env.execute("Operator State Demo")
    }
}

/**
 * 自定义实现算子状态的使用：需要混入ListCheckpointed
 * 注意：由于不通过上下文环境中获取状态，服务挂掉后状态数据会丢失，
 *      此时需要混入ListCheckpointed，通过引入checkpoint机制来实现自定义算子状态的更新及恢复机制。
 */
class MyMapper() extends RichMapFunction[SensorReading, Long] with ListCheckpointed[Long] {

    /**
     * 算子状态：task范围内的共享状态，不需从上下文中获取配置
     */
    var count: Long = 0L

    /**
     * map算子处理函数
     * @param value
     * @return
     */
    override def map(value: SensorReading): Long = {
        count += 1
        count
    }

    /**
     * 更新算子状态
     * @param checkpointId
     * @param timestamp
     * @return
     */
    override def snapshotState(checkpointId: Long, timestamp: Long): util.List[Long] = {
        val stateList = new util.ArrayList[Long]()
        stateList.add(count)
        stateList
    }

    /**
     * 故障时，从状态后端恢复状态数据
     * @param state
     */
    override def restoreState(state: util.List[Long]): Unit = {
        for (countState <- state) {
            count += countState
        }
    }
}
