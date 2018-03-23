package com.flink.main;

import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink;
import org.apache.flink.streaming.connectors.fs.StringWriter;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.fs.bucketing.DateTimeBucketer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import com.google.gson.Gson;

/**
 * Generates tuples by accepting a stream of inputs, and performing
 * enrichment, transformation and indexing
 * @author shankarganesh
 *
 */
public class FeedStreamAsync {

	public static void main(String[] args) throws Exception {

		// the port to connect to
		final int port = 9000;
//		try {
//			final ParameterTool params = ParameterTool.fromArgs(args);
//			port = params.getInt("port");
//		} catch (Exception e) {
//			System.err.println("No port specified. Please run 'SocketWindowWordCount --port <port>'");
//			return;
//		}

		// get the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		// set time characteristic : TODO : setting it to processingTime but eventTime might make more sense
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
		
		// enable checkpointing
		env.enableCheckpointing(500);
		
		// get input data by connecting to the socket
		DataStream<String> stream = env.socketTextStream("localhost", port, "\n");
		
		DataStream<ItemModel> resultStream = AsyncDataStream.unorderedWait(stream, new FeedStreamEnrichment(),
				120, TimeUnit.SECONDS, 100000);
			
		// generating keyed stream based on category id
		KeyedStream<ItemModel, Integer> keyStream = resultStream.keyBy(new KeySelector<ItemModel, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
			public Integer getKey(ItemModel value) throws Exception {
				// TODO Auto-generated method stub
				return value.getCategoryId();
			}
			
		});
		
//		 // parse the data, group it, window it, and aggregate the counts
//        DataStream<ItemFileModel> fileStream = resultStream
//            .flatMap(new FlatMapFunction<ItemModel, ItemFileModel>() {
//               @Override
//				public void flatMap(ItemModel value, org.apache.flink.util.Collector<ItemFileModel> out) throws Exception {
//					
//            	   		ItemFileModel tuple = new ItemFileModel(new IntWritable(Integer.valueOf(value.getId())), new Text(value.getTitle()), new IntWritable(1));
//            	   		out.collect(tuple);
//				}
//            })
//            .keyBy("itemId")
//            .timeWindow(Time.seconds(5))
//            .reduce(new ReduceFunction<ItemFileModel>() {
//                @Override
//                public ItemFileModel reduce(ItemFileModel a, ItemFileModel b) {
//                    return new ItemFileModel(a.itemId, a.title, new IntWritable(a.count.get() + b.count.get()));
//                }
//            });

        
        BucketingSink<ItemModel> sink = new BucketingSink<ItemModel>("/Users/shankarganesh/path");
        sink.setBucketer(new DateTimeBucketer<ItemModel>("yyyy-MM-dd--HHmm"));
        sink.setWriter(new StringWriter<ItemModel>());
        sink.setBatchSize(1024 * 1024 * 400); // this is 400 MB,
        keyStream.addSink(sink);
        
		// init sink utils
		ESSinkUtils esSinkUtils = new ESSinkUtils();
		
		resultStream.addSink(new ElasticsearchSink<>(esSinkUtils.createConfigMap(), esSinkUtils.createSocketAddresses(), new ElasticsearchSinkFunction<ItemModel>() {
		    
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public IndexRequest createIndexRequest(ItemModel element) {
				Gson gson = new Gson();
				return Requests.indexRequest()
		                .index(Constants.INDEX_NAME)
		                .type(Constants.TYPE_NAME)
		                .source(gson.toJson(element));
		    }
		    
		    @Override
		    public void process(ItemModel element, RuntimeContext ctx, RequestIndexer indexer) {
		        indexer.add(createIndexRequest(element));
		    }
		}));

		env.execute("Async Stream Enrichment and Indexing");
	}
}
