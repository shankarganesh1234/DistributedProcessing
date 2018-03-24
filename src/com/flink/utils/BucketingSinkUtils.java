package com.flink.utils;

import org.apache.flink.streaming.connectors.fs.StringWriter;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;

import com.flink.bucketer.CustomBucketer;
import com.flink.models.ItemModel;

/**
 * Creates a bucketing sink instance with defined config params
 * @author shankarganesh
 *
 */
public class BucketingSinkUtils {

	public BucketingSink<ItemModel> getBucketingSink() {
		BucketingSink<ItemModel> sink = new BucketingSink<ItemModel>("/Users/shankarganesh/path");
		sink.setBucketer(new CustomBucketer("yyyy-MM-dd--HHmm"));
		sink.setWriter(new StringWriter<ItemModel>());
		sink.setBatchSize(1024 * 1024 * 400); // this is 400 MB,
		return sink;
	}
}
