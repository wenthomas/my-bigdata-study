package com.wenthomas.flink.checkpoint

import java.util.concurrent.TimeUnit

import com.wenthomas.flink.api.source.SensorReading
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.CheckpointingMode
/**
 * @author Verno
 * @create 2020-04-24 3:44 
 */
/**
 * CheckPoint配置
 */
object CheckpointSetting {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)
        //配置状态后端
        env.setStateBackend( new FsStateBackend("") )

        //----------------------------------------checkpoint相关配置-----------------------------------------------------
        //checkpoint相关配置
        //启用检查点，指定触发检查点的间隔时间（毫秒）
        env.enableCheckpointing(1000L)

        //1,检查点配置级别
        env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

        //2,配置检查点超时时间
        env.getCheckpointConfig.setCheckpointTimeout(30000L)

        //3,配置可同时进行的checkpoint数
        env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)

        //4,配置两个checkpoint之间的最小时间间隔
        env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500L)

        //5,配置故障时是否使用最近的checkpoint来进行恢复
        //true：使用最近checkpoint恢复； false:使用最近自定义的savepoint来恢复
        env.getCheckpointConfig.setPreferCheckpointForRecovery(false)

        //6,配置允许checkpoint失败的次数，可重试的次数
        env.getCheckpointConfig.setTolerableCheckpointFailureNumber(3)

        //------------------------------------------重启策略配置---------------------------------------------------------
        //(最常见)fixedDelayRestart按照固定延迟重启：尝试重启次数为3，时间间隔为10s
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000L))
        //failureRateRestart失败率重启：5分钟的时间窗口内失败次数达到5次的情况下重启，每次重启尝试的间隔为10s
        env.setRestartStrategy(RestartStrategies.failureRateRestart(5, Time.of(5, TimeUnit.MINUTES), Time.of(10, TimeUnit.SECONDS)))
        //不配置重启策略，直接返回到上一层
        env.setRestartStrategy(RestartStrategies.fallBackRestart())
        //配置失败不理会不反馈，不使用checkpoint相关机制
        env.setRestartStrategy(RestartStrategies.noRestart())

        //--------------------------------------------------------------------------------------------------------------
        //val inputStream = env.readTextFile("E:\\project\\my-bigdata-study\\my-flink-study\\src\\main\\resources\\sensor.txt")
        val inputStream = env.socketTextStream("mydata01", 7777)
        val dataStream = inputStream
                .map({
                    data => {
                        val dataArray = data.split(", ")
                        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
                    }
                })

        dataStream.print()
        env.execute("Checkpoint Config Demo")
    }
}
