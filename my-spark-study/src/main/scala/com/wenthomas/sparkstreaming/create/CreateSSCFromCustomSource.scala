package com.wenthomas.sparkstreaming.create

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * @author Verno
 * @create 2020-03-23 10:50 
 */
object CreateSSCFromCustomSource {

    def main(args: Array[String]): Unit = {
        val conf = new SparkConf().setMaster("local[*]").setAppName("CreateSSCFromCustomSource")
        val ssc = new StreamingContext(conf, Seconds(3))

        val sourceStream = ssc.receiverStream(new MySource("mydata01", 9999))

        sourceStream
                .flatMap(_.split(" "))
                .map((_, 1))
                .reduceByKey(_ + _)
                .print

        ssc.start()
        ssc.awaitTermination()
    }

}

/**
 * 自定义接收器：模拟从socket接收数据
 * @param host
 * @param port
 */
class MySource(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_ONLY) {

    /**
     * 提出来socket和reader方便onStop方法释放资源
     */
    var socket: Socket = _
    var reader: BufferedReader = _

    /**
     * 1，启动一个子线程来接收数据
     * 2，接收的数据通过调用store(data)传递给其他执行器进行处理
     * 3，如果发生异常，重启接收器restart，会自动的按照顺序调用onStop和onStart
     */
    override def onStart(): Unit = {
        runInThread {
            //连接socket，去读取数据
            try {
                socket = new Socket(host, port)
                val isr = new InputStreamReader(socket.getInputStream, "utf-8")
                reader = new BufferedReader(isr)

                var line = reader.readLine()

                while (line != null && socket.isConnected) {
                    store(line)
                    //如果流中没有数据，下一行会一直阻塞
                    line = reader.readLine()
                }
            } catch {
                case e => println(e.getMessage)
            } finally {
                restart("重启接收器")
                //自动立即调用onStop方法，再调用onStart方法
            }
        }
    }

    /**
     * 在一个子线程中执行传入的代码
     */
    def runInThread(op: => Unit): Unit = {
        new Thread("Socket Receiver") {
            setDaemon(true)
            override def run(): Unit = op
        }.start()
    }

    /**
     * 释放资源
     */
    override def onStop(): Unit = {
        if (null != socket) socket.close()
        if (null != reader) reader.close()
    }
}
