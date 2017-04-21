package com.casicloud.aop.kafka.consumer;

import org.springframework.context.support.ClassPathXmlApplicationContext;

public class ConsumerHdfsTest {
	
	@SuppressWarnings("resource")
	public static void main(String[] args) {
		ClassPathXmlApplicationContext applicationContext=new ClassPathXmlApplicationContext(new String[]{"spring-consumer-hdfs.xml"});
		applicationContext.start();
		
	}
}
