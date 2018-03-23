package com.flink.main;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class ItemFileModel {

	IntWritable itemId;
	Text title;
	IntWritable count;
	
	public ItemFileModel(IntWritable itemId, Text title, IntWritable count) {
		super();
		this.itemId = itemId;
		this.title = title;
		this.count = count;
	}
	
	
}
