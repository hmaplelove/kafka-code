package com.casicloud.aop.kafka.consumer;

import org.springframework.context.support.ClassPathXmlApplicationContext;

public class ConsumerStormTest {
	
	@SuppressWarnings("resource")
	public static void main(String[] args) {
		ClassPathXmlApplicationContext applicationContext=new ClassPathXmlApplicationContext(new String[]{"applicationContext.xml"});
		applicationContext.start();
	}
}
