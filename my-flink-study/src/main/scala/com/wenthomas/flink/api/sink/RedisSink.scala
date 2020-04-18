package com.wenthomas.flink.api.sink
import com.wenthomas.flink.api.source.SensorReading
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.redis.RedisSink
import org.apache.flink.streaming.connectors.redis.common.config.{FlinkJedisConfigBase, FlinkJedisPoolConfig}
import org.apache.flink.streaming.connectors.redis.common.mapper.{RedisCommand, RedisCommandDescription, RedisMapper}
/**
 * @author Verno
 * @create 2020-04-18 9:33 
 */
/**
 * Sink：将数据流输出到Redis
 * 注意：需要手写定义FlinkJedisPoolConfig配置类以及重写RedisMapper
 */
object RedisSink {
    def main(args: Array[String]): Unit = {
        val env = StreamExecutionEnvironment.getExecutionEnvironment
        env.setParallelism(1)

        val inputStream = env.readTextFile("E:\\project\\my-bigdata-study\\my-flink-study\\src\\main\\resources\\sensor.txt")
        val dataStream = inputStream
                .map({
                    data => {
                        val dataArray = data.split(", ")
                        SensorReading(dataArray(0), dataArray(1).toLong, dataArray(2).toDouble)
                    }
                })

        //1,定义一个Redis的配置类
        val conf = new FlinkJedisPoolConfig.Builder()
                .setHost("localhost")
                .setPort(6379)
                .build()

        //2,定义一个RedisMapper用于编写对Redis的操作
        val myMapper = new RedisMapper[SensorReading] {
            //定义保存数据到Redis的命令, hset table_name key value
            override def getCommandDescription: RedisCommandDescription = {
                new RedisCommandDescription(RedisCommand.HSET, "sensor_temperature")
            }

            override def getKeyFromData(t: SensorReading): String = {
                t.id
            }

            override def getValueFromData(t: SensorReading): String = {
                t.temperature.toString
            }
        }

        //3,将数据流输出写入到Redis
        dataStream.addSink(new RedisSink[SensorReading](conf, myMapper))

        env.execute("Redis Sink Demo")
    }
}

