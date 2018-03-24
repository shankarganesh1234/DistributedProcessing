package com.flink.reducer;

import org.apache.flink.api.common.functions.ReduceFunction;

/**
 * Custom ReduceFunction to remove duplicates within a defined time window
 * @author shankarganesh
 *
 */
public class DuplicationReducer implements ReduceFunction<String>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public String reduce(String value1, String value2) throws Exception {
		return value2;
	}

}
