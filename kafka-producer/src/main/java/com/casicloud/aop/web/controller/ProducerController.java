package com.casicloud.aop.web.controller;

import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.integration.kafka.support.KafkaHeaders;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import com.alibaba.fastjson.JSON;
import com.casicloud.aop.kafka.producer.DataUtils;

@Controller
@RequestMapping("/api")
public class ProducerController {
	@Autowired
    @Qualifier("inputToKafka")
    private MessageChannel channel;
	@RequestMapping("/iotData")
	@ResponseBody
	public Boolean  send(HttpServletRequest request,HttpServletResponse response) throws Exception{
		boolean flag=true;
		for (Map<Object, Object> data : DataUtils.grenData(100l)) {
			Message<String> msg = MessageBuilder.withPayload(JSON.toJSONString(data))
					//.setHeader(KafkaHeaders.MESSAGE_KEY, data.get("key").toString())
					//.setHeader(KafkaHeaders.PARTITION_ID, IotPartitioner.keyMap.get(data.get("key")))
					.setHeader(KafkaHeaders.TOPIC, "IOT_DATA").build();
			boolean b=channel.send(msg);
			if (!b) {
				flag=b;
			}
			System.out.println(data.toString());
		}
		return flag;
		
	}
}
