package com.casicloud.aop.kafka.core.service.impl;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;

import com.alibaba.fastjson.JSON;
import com.casicloud.aop.kafka.core.service.KafkaService;

public class KafkaMongoService implements KafkaService{
	
	private static final Logger logger = LoggerFactory.getLogger(KafkaMongoService.class);
	private static SimpleDateFormat sdf_full=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
	
	@Autowired
	private MongoTemplate mongoTemplate;
	
	@Override
	public void processMessage(Map<Object, Map<Object, Object>> message) throws Exception {
		onMessage(message);
	}
	
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void onMessage(Map<Object, Map<Object, Object>> message) throws Exception {
		for (Map.Entry < Object,Map<Object, Object>>entry:  message.entrySet()){
			String topic=entry.getKey().toString();
            for (Entry<Object, Object> msg : entry.getValue().entrySet()) {
            	List<String> list=(List<String>) msg.getValue();
            	for (String json : list) {
            		HashMap data=JSON.parseObject(json, HashMap.class);
            		String equipment=data.get("equipment").toString();
            		String t=data.get("t").toString();
            		String createTime=data.get("createTime").toString();
            		long t_date=Long.valueOf(t);
            		long c_date=Long.valueOf(createTime);
            		data.put("t", sdf_full.format(t_date));
            		data.put("createTime", sdf_full.format(c_date));
            		String collectionName="e"+equipment;
            		if (!mongoTemplate.collectionExists(collectionName)) {
            			mongoTemplate.createCollection(collectionName);
					}
            		mongoTemplate.save(data, collectionName);
	            	logger.info("["+topic+"]=========>"+JSON.toJSONString(data));
				}
            }
        }
	
	}

}
