package com.flink.keyselector;

import org.apache.flink.api.java.functions.KeySelector;

import com.flink.models.ItemModel;

/**
 * Key selector for ItemModel
 * 
 * @author shankarganesh
 *
 */
public class ItemKeySelector implements KeySelector<ItemModel, Integer>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Integer getKey(ItemModel value) throws Exception {
		return value.getCategoryId();
	}

}
