package com.casicloud.aop.kafka.core.service.impl;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.hbase.HbaseTemplate;
import org.springframework.data.hadoop.hbase.TableCallback;

import com.alibaba.fastjson.JSON;
import com.casicloud.aop.kafka.core.service.KafkaService;

@SuppressWarnings("deprecation")
public class KafkaHBaseService implements KafkaService{
	private static final Logger logger = LoggerFactory.getLogger(KafkaHBaseService.class);
	private static SimpleDateFormat sdf_full=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
	@Autowired
	private HbaseTemplate  hbaseTemplate;
	@Override
	public void processMessage(Map<Object, Map<Object, Object>> message) throws Exception {
		onMessage(message);
	}
	
	@SuppressWarnings({ "unchecked" })
	private void onMessage(Map<Object, Map<Object, Object>> message) throws Exception {
		for (Map.Entry < Object,Map<Object, Object>>entry:  message.entrySet()){
			final String topic=entry.getKey().toString();
            for (Entry<Object, Object> msg : entry.getValue().entrySet()) {
            	List<String> list=(List<String>) msg.getValue();
            	for (String json : list) {
            		HashMap<Object, Object> data=JSON.parseObject(json, HashMap.class);
            		String equipment=data.get("equipment").toString();
            		final String t=data.get("t").toString();
            		String createTime=data.get("createTime").toString();
            		long t_date=Long.valueOf(t);
            		long c_date=Long.valueOf(createTime);
            		final String t_str=sdf_full.format(t_date);
            		data.put("t", t_str);
            		data.put("createTime", sdf_full.format(c_date));
            		String tableName="e"+equipment;
            		final HashMap<Object, Object> params=data;
            		String key=hbaseTemplate.execute(tableName, new TableCallback<String>() {  
						public String doInTable(HTableInterface table) throws Throwable {  
                        	String key=UUID.randomUUID().toString();
                        	key=t_str+"-"+key;
                            try{  
                                Put put = new Put(key.getBytes());
                                for (Entry<Object, Object> entry : params.entrySet()) {
                                	put.addColumn(Bytes.toBytes(topic),Bytes.toBytes(entry.getKey().toString()), Bytes.toBytes(entry.getValue().toString()));  
								}
                                table.put(put);  
                            }catch(Exception e){  
                                e.printStackTrace();  
                            }
							return key;  
                        }  
                    });
            		logger.info("[rowKey]=========>"+key);
	            	logger.info("["+topic+"]=========>"+JSON.toJSONString(data));
				}
            }
        }
	
	}

}
