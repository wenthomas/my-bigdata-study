package com.wenthomas.flume.source.lesson;

import java.util.ArrayList;
import java.util.List;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

/*
 * 使用flume接收数据，并给每条数据添加前缀，输出到控制台。前缀可从flume配置文件中配置
 */
public class MySource  extends AbstractSource implements Configurable, PollableSource {

	private String prefix;
	// 最核心方法，在process()中，创建Event，将event放入channel
	// Status{ READY, BACKOFF}
	// READY: source成功第封装了event，存入到channel，返回READY
	// BACKOFF： source无法封装了event，无法存入到channel，返回BACKOFF
	// process()方法会被Source所在的线程循环调用！

	public Status process() throws EventDeliveryException {
		
		Status status=Status.READY;
		
		//封装event
		List<Event> datas=new ArrayList();
		
		for (int i = 0; i < 10; i++) {
			
			SimpleEvent e = new SimpleEvent();
			
			//向body中封装数据
			e.setBody((prefix+"hello"+i).getBytes());
			
			datas.add(e);
			
		}
		
		//将数据放入channel
		// 获取当前source对象对应的channelprocessor
		try {
			
			Thread.sleep(5000);
			
			ChannelProcessor cp = getChannelProcessor();
			
			cp.processEventBatch(datas);
			
		} catch (Exception e) {
			
			status=Status.BACKOFF;
			
			e.printStackTrace();
		}
		
		return status;
	}

	// 当source没有数据可封装时，会让source所在的线程先休息一会，休息的时间，由以下值*计数器系数
	public long getBackOffSleepIncrement() {
		return 2000;
	}


	public long getMaxBackOffSleepInterval() {
		return 5000;
	}

	// 从配置中来读取信息
	public void configure(Context context) {
		
		//从配置文件中读取key为prefix的属性值，如果没有配置，提供默认值atguigu:
		prefix=context.getString("prefix", "atguigu:");
		
	}

}
