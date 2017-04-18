package com.casicloud.aop.kafka.core.partitioner;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import  com.casicloud.aop.kafka.producer.DataUtils;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class IotPartitioner implements Partitioner {
	
	private static final Logger LOGGER=LoggerFactory.getLogger(IotPartitioner.class);
	
	public IotPartitioner() {
		
	}
	public IotPartitioner(VerifiableProperties props) {
		
	}
	public static Map<String, Integer> keyMap=new HashMap<String,Integer>();
	static{
		for (int i=0;i<DataUtils.keys.length;i++) {
			keyMap.put(DataUtils.keys[i], i);
		}
	}
	
	@Override
	public int partition(Object key, int numPartitions) {
		
		if (keyMap.containsKey(key)) {
			LOGGER.info(key+"=========>"+keyMap.get(key));
			return keyMap.get(key);
		}
		return 0;
	}

}
