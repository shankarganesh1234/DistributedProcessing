package com.flink.keyselector;

import org.apache.flink.api.java.functions.KeySelector;

/**
 * Key selector for incoming string messages
 * @author shankarganesh
 *
 */
public class StringKeySelector implements KeySelector<String, String>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public String getKey(String value) throws Exception {
		return value;
	}

}
