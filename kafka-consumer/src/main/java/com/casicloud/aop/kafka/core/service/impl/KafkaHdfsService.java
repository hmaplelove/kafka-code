package com.casicloud.aop.kafka.core.service.impl;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;

import com.alibaba.fastjson.JSON;
import com.casicloud.aop.kafka.core.service.KafkaService;
import com.casicloud.aop.kafka.utils.HDFSClinet;

public class KafkaHdfsService implements KafkaService{

	private static SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd");
	
	@Autowired 
	HDFSClinet HdfsClinet;
	private static final Logger logger = LoggerFactory.getLogger(KafkaHdfsService.class);
	
	@Override
	public void processMessage(Map<Object, Map<Object, Object>> message) throws IOException{
		onMessage(message);
	}
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void onMessage(Map<Object, Map<Object, Object>> message) throws IOException {
		
		for (Map.Entry < Object,Map<Object, Object>>entry:  message.entrySet()){
            String topic=entry.getKey().toString();
            for (Entry<Object, Object> msg : entry.getValue().entrySet()) {
            	List<String> list=(List<String>) msg.getValue();
            	for (String json : list) {
            		HashMap data=JSON.parseObject(json, HashMap.class);
            		String orgId=data.get("orgId").toString();
            		String equipment=data.get("equipment").toString();
            		String collecttime=data.get("t").toString();
            		long date=Long.valueOf(collecttime);
	            	String time=sdf.format(date);
	            	String dir=new StringBuffer("/").append("IOT").append("/").append(orgId).append("/e").append(equipment).append("/").append(topic).append("/").append(time).toString();
	            	HdfsClinet.createDirectory(dir);
	            	String filePath=new StringBuffer(dir).append("/").append(data).append(".json").toString();
	            	HdfsClinet.appendToFile(filePath, json);
            		logger.info("["+topic+"]=========>"+json);
				}
            }
        }
	}

}
