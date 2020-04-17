package com.wenthomas.flink.wordcount

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @author Verno
 * @create 2020-04-15 14:14 
 */
object StreamWordCount {
    def main(args: Array[String]): Unit = {
        //1,创建一个flink流处理的执行环境
        val env = StreamExecutionEnvironment.getExecutionEnvironment

        import org.apache.flink.api.scala._

        val params = ParameterTool.fromArgs(args)
        val hostname = params.get("host")
        val port = params.getInt("port")

        //2,获取流数据，格式为DataStream：接收socket文本流
        val inputDataStream = env.socketTextStream(hostname, port)

        //3,基于DataStream做转换计算
        //先按空格分词打散，然后按照word为key做分组，最后做聚合
        val resultDataStream = inputDataStream.flatMap(line => line.split("\\W+"))
                .filter(_.nonEmpty)
                .map(word => (word, 1))
                .keyBy(0)
                .sum(1)

        //4,打印输出
        resultDataStream.print()

        //5,执行job
        env.execute("stream word count job")


    }

}
