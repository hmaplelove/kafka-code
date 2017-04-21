package com.casicloud.aop.kafka.consumer;

import org.springframework.context.support.ClassPathXmlApplicationContext;

public class ConsumerMongoTest {
	
	@SuppressWarnings("resource")
	public static void main(String[] args) {
		ClassPathXmlApplicationContext applicationContext=new ClassPathXmlApplicationContext(new String[]{"spring-consumer-mongo.xml"});
		applicationContext.start();
		
	}
}
