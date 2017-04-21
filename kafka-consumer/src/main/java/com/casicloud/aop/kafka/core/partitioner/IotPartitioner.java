package com.casicloud.aop.kafka.core.partitioner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import kafka.producer.Partitioner;
import kafka.utils.VerifiableProperties;

public class IotPartitioner implements Partitioner {
	
	private static Logger logger=LoggerFactory.getLogger(IotPartitioner.class);
	
	public IotPartitioner() {
		
	}
	
	public IotPartitioner(VerifiableProperties props) {
		
	}
	
	@Override
	public int partition(Object key, int numPartitions) {
		logger.debug(key.toString());
		return 0;
	}

}
