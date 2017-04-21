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
	
	private static final Logger logger = LoggerFactory.getLogger(KafkaHdfsService.class);
	private static SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd");
	private static SimpleDateFormat sdf_full=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
	
	@Autowired 
	private HDFSClinet HdfsClinet;
	
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
            		
            		String t=data.get("t").toString();
            		String createTime=data.get("createTime").toString();
            		
            		long t_date=Long.valueOf(t);
            		long c_date=Long.valueOf(createTime);
            		
            		data.put("t", sdf_full.format(t_date));
            		data.put("createTime", sdf_full.format(c_date));
            		
	            	String time=sdf.format(t_date);
	            	
	            	String dir=new StringBuffer("/").append("IOT").append("/").append(orgId).append("/e").append(equipment).append("/").append(topic).append("/").append(time).toString();
	            	HdfsClinet.createDirectory(dir);
	            	String filePath=new StringBuffer(dir).append("/").append("data").append(".json").toString();
	            	HdfsClinet.appendToFile(filePath, new StringBuffer(JSON.toJSONString(data)).append("\r\n").toString());
            		logger.info("["+topic+"]=========>"+JSON.toJSONString(data));
				}
            }
        }
	}

}
