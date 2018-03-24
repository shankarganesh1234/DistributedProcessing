package com.flink.main;

import java.util.concurrent.TimeUnit;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;

import com.flink.enrichment.FeedStreamEnrichment;
import com.flink.keyselector.StringKeySelector;
import com.flink.models.ItemModel;
import com.flink.reducer.DuplicationReducer;
import com.flink.utils.BucketingSinkUtils;

/**
 * Generates tuples by accepting a stream of inputs, and performing enrichment,
 * transformation and indexing
 * 
 * The process can be divided into the following steps :-
 * 	Accept inputs from console. You can use netcat : nc -l <port> to accept inputs as a stream
 * 	Dedupe the incoming stream for <duration>
 * 	Enrich the deduped stream : Call web service, transform and collect
 * 	Write the result stream contents to sink
 * 
 * @author shankarganesh
 *
 */
public class FeedStreamAsync {

	public static void main(String[] args) throws Exception {

		// the port to connect to
		final int port;
		try {
			final ParameterTool params = ParameterTool.fromArgs(args);
			port = params.getInt("port");
		} catch (Exception e) {
			System.err.println("No port specified. Please run 'SocketWindowWordCount --port <port>'");
			return;
		}

		// get the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// set time characteristic : TODO : setting it to processingTime but eventTime
		// might make more sense
		env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

		// enable checkpointing
		env.enableCheckpointing(10000);

		// get input data by connecting to the socket
		DataStream<String> stream = env.socketTextStream("localhost", port, "\n");

		// dedupe incoming items within a pre defined time window
		DataStream<String> dedupedStream = stream.keyBy(new StringKeySelector()).timeWindow(Time.minutes(1)).reduce(new DuplicationReducer());

		// enrich the deduped stream
		DataStream<ItemModel> resultStream = AsyncDataStream.unorderedWait(dedupedStream, new FeedStreamEnrichment(),
				120, TimeUnit.SECONDS, 100000);

		
		// *************** Section for writing data to sink *********************//
		
		// print stream contents to console
		resultStream.print().setParallelism(1);
		
		/**
		 * Write to BucketingSink
		 * Configure the bucketing sink within the methods in BucketingSinkUtils()
		 * Current path is {basePath} / {yyyyMMdd--HHmm} / {categoryId}
		 * The path can be configured in the com.flink.bucketer.CustomBucketer
		 */
		
		BucketingSinkUtils bucketingSinkUtils = new BucketingSinkUtils();
		BucketingSink<ItemModel> sink = bucketingSinkUtils.getBucketingSink();
		resultStream.addSink(sink);

		
		/**
		 * Write to ElasticSearch Sink
		 * Comment the  below section if you are running locally and do not have elasticsearch set up
		 * 
		 */
		// init sink utils
		// ESSinkUtils esSinkUtils = new ESSinkUtils();
		//
		// resultStream.addSink(new ElasticsearchSink<>(esSinkUtils.createConfigMap(),
		// esSinkUtils.createSocketAddresses(), new
		// ElasticsearchSinkFunction<ItemModel>() {
		//
		// /**
		// *
		// */
		// private static final long serialVersionUID = 1L;
		//
		// public IndexRequest createIndexRequest(ItemModel element) {
		// Gson gson = new Gson();
		// return Requests.indexRequest()
		// .index(Constants.INDEX_NAME)
		// .type(Constants.TYPE_NAME)
		// .source(gson.toJson(element));
		// }
		//
		// @Override
		// public void process(ItemModel element, RuntimeContext ctx, RequestIndexer
		// indexer) {
		// indexer.add(createIndexRequest(element));
		// }
		// }));

		env.execute("Async Stream Enrichment and Indexing");
	}
}
