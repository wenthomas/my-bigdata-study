package com.wenthomas.flink.state

import com.wenthomas.flink.api.source.SensorReading
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.runtime.state.memory.MemoryStateBackend
/**
 * @author Verno
 * @create 2020-04-21 14:29 
 */
/**
 * flink几种状态后端：内存级、文件级、RocksDB
 */
object Backend {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        //---------------------------------------状态后端引入-------------------------------------------------------------
        //1，MemoryStateBackend
//        env.setStateBackend(new MemoryStateBackend())
        //2, FsStateBackend
//        env.setStateBackend(new FsStateBackend(""))
        //3, RocksDBStateBackend，需要添加依赖包
//        env.setStateBackend(new RocksDBStateBackend("", true))

        //val inputStream = env.readTextFile("E:\\project\\my-bigdata-study\\my-flink-study\\src\\main\\resources\\sensor.txt")
        val inputStream = env.socketTextStream("mydata01", 7777)
        val dataStream = inputStream
                .map({
                    data => {
                        val dataArray = data.split(", ")
                        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
                    }
                })

        env.execute("Backend Demo")
    }

}
