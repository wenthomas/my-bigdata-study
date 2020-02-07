package com.wenthomas.flume.interceptor.lesson;

import java.util.List;
import java.util.Map;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;

// 为每个event的header中添加key-value:  time=时间戳
public class MyInterceptor implements Interceptor{

	//初始化
	public void initialize() {
		
	}

	//拦截处理方法
	// 为每个event的header中添加key-value:  time=时间戳
	public Event intercept(Event event) {
		
		Map<String, String> headers = event.getHeaders();
		
		headers.put("time", System.currentTimeMillis()+"");
		
		return event;
	}

	//拦截处理方法
	public List<Event> intercept(List<Event> events) {
		
		for (Event event : events) {
			intercept(event);
		}
		
		return events;
	}

	// 结合时调用的方法
	public void close() {
		
	}
	
	//额外提供一个内部的Builder，因为Flume在创建拦截器对象时，固定调用Builder来获取
	public static class Builder implements Interceptor.Builder{

		// 读取配置文件中的参数
		public void configure(Context context) {
			
		}

		//返回一个当前的拦截器对象
		public Interceptor build() {
			return new MyInterceptor();
		}
		
		
	}

}
