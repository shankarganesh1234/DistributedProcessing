package com.flink.main;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility for creating ES Sink related configs 
 * @author shankarganesh
 *
 */
public class ESSinkUtils {

	/**
	 * Returns a config map for elasticsearch sink
	 * @return
	 */
	public Map<String, String> createConfigMap() {
		Map<String, String> config = new HashMap<>();
		config.put("cluster.name", "elasticsearch_shankarganesh");
		
		// This instructs the sink to emit after every element, otherwise they would be buffered
		config.put("bulk.flush.max.actions", "1");
		return config;
	}
	
	/**
	 * Returns a list of transport addresses to bind
	 * @return
	 * @throws UnknownHostException
	 */
	public List<InetSocketAddress> createSocketAddresses() throws UnknownHostException {
		List<InetSocketAddress> transportAddresses = new ArrayList<>();
		transportAddresses.add(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9300));
		return transportAddresses;
	}
	
}
