package com.wenthomas.flink.wordcount

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.api.scala.ExecutionEnvironment
/**
 * @author Verno
 * @create 2020-04-15 11:45 
 */
/**
 * 批处理 Word count
 */
object BatchWordCount {
    def main(args: Array[String]): Unit = {
        //1,创建一个flink批处理的执行环境
        val env = ExecutionEnvironment.getExecutionEnvironment

        //2,从文件中读取数据，格式为DataSet
        val inputDataSet = env.readTextFile("E:\\project\\my-bigdata-study\\my-flink-study\\src\\main\\resources\\word.txt")

        //3,基于DataSet做转换计算
        //先按空格分词打散，然后按照word为key做分组，最后做聚合
        import org.apache.flink.api.scala._
        val resultDataSet = inputDataSet.flatMap(line => line.split("\\W+"))
                .map(word => (word, 1))
                .groupBy(0)
                .sum(1)

        //4,打印输出
        resultDataSet.print()

    }

}
