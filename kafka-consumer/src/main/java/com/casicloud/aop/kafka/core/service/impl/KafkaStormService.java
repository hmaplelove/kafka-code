package com.casicloud.aop.kafka.core.service.impl;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.integration.kafka.support.KafkaHeaders;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;

import com.alibaba.fastjson.JSON;
import com.casicloud.aop.kafka.core.service.KafkaService;

@SuppressWarnings({ "unchecked", "rawtypes" })
public class KafkaStormService implements KafkaService{
	
	private static final Logger logger = LoggerFactory.getLogger(KafkaStormService.class);
	private static Map<String, HashMap> dsMap = new HashMap<String, HashMap>();
	private static SimpleDateFormat sdf_date_hh=new SimpleDateFormat("yyyyMMddHH");
	private static SimpleDateFormat sdf_full=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
	
	@Autowired
    @Qualifier("inputToKafka")
	private MessageChannel channel;
	
	@Override
	public void processMessage(Map<Object, Map<Object, Object>> message) throws Exception{
		onMessage(message);
	}
	private void onMessage(Map<Object, Map<Object, Object>> message) throws Exception {
		for (Map.Entry < Object,Map<Object, Object>>entry:  message.entrySet()){
            String topic=entry.getKey().toString();
            for (Entry<Object, Object> msg : entry.getValue().entrySet()) {
            	List<String> list=(List<String>) msg.getValue();
            	for (String json : list) {
            		logger.info("["+topic+"]=========>"+json);
            		HashMap data=JSON.parseObject(json, HashMap.class);
            		String equipment=data.get("equipment").toString();
            		String collecttime=data.get("t").toString();
            		long current=Long.valueOf(collecttime);
            		//yyyyMMddHH
            		String current_date_hh=sdf_date_hh.format(new Date(current));
            		
            		if (!dsMap.containsKey(equipment)) {
            			dsMap.put(equipment, new HashMap());
					}
            		//删除历史的数据
            		long yesterday=current-24*60*60*1000;
            		String yesterday_date_hh=sdf_date_hh.format(new Date(yesterday));
            		if (dsMap.get(equipment).containsKey(yesterday_date_hh)) {
            			dsMap.get(equipment).remove(yesterday_date_hh);
            		}
            		long std_millis=sdf_date_hh.parse(current_date_hh).getTime();
            		long min_std=std_millis-150*1000;
            		long max_std=std_millis+150*1000;
            		if (min_std<=current&&current<=max_std) {
						//
            			String t=data.get("t").toString();
                		String createTime=data.get("createTime").toString();
                		
                		long t_date=Long.valueOf(t);
                		long c_date=Long.valueOf(createTime);
                		
                		data.put("t", sdf_full.format(t_date));
                		data.put("createTime", sdf_full.format(c_date));
                		
            			if(!dsMap.get(equipment).containsKey(current_date_hh)){
            				dsMap.get(equipment).put(current_date_hh, json);
            				send(JSON.toJSONString(data),topic);
            			}
					}
				}
            }
        }
	}
	
	private boolean send(String data,String topic) throws Exception{
		logger.info("send to storm["+topic+"_STORM]=========>"+data);
		Message<String> msg = MessageBuilder.withPayload(data)
				.setHeader(KafkaHeaders.TOPIC,topic+"_STORM").build();
		return channel.send(msg);
	
	}
}
