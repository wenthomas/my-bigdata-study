package com.wenthomas.flink.api.environment

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @author Verno
 * @create 2020-04-17 11:39 
 */
object GetExecutionEnvironment {
    def main(args: Array[String]): Unit = {
        //0,创建执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        env.execute()
    }
}
