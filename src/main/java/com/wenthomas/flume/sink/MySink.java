package com.wenthomas.flume.sink;

import org.apache.flume.*;
import org.apache.flume.conf.Configurable;
import org.apache.flume.sink.AbstractSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Verno
 * @create 2020-01-15 11:52
 */

// 从配置文件中读取一个后缀，将event的内容读取后，拼接后缀进行输出
public class MySink extends AbstractSink implements Configurable {

    private String suffix;
    private Logger logger= LoggerFactory.getLogger(MySink.class);

    //核心方法：处理sink逻辑
    // Status.ready:  成功传输了一个或多个event
    // Status.backoff:  从channel中无法获取数据
    public Status process() throws EventDeliveryException {
        Status status = Status.READY;
        //获取当前sink对接的channel
        Channel channel = getChannel();

        //声明Event，用来接收chanel中的event
        Event event = null;

        //获取事务
        Transaction transaction = channel.getTransaction();
        try {
            //开启事务
            transaction.begin();

            //如果channel中，没有可用的event，此时e会是null
            event = channel.take();

            if (null == event) {
                status = Status.BACKOFF;
            } else {
                logger.info(new String(event.getBody())+suffix);
            }
            //提交事务
            transaction.commit();
        } catch (ChannelException e1) {
            //回滚事务
            transaction.rollback();

            status = Status.BACKOFF;

            e1.printStackTrace();
        } finally {
            //关闭事务对象
            transaction.close();
        }
        return status;
    }

    //从配置中读取配置的参数
    public void configure(Context context) {
        suffix = context.getString("suffix", ":suffix");
    }
}
