package com.flink.main;

import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch5.ElasticsearchSink;
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

		// get input data by connecting to the socket
		DataStream<String> stream = env.socketTextStream("localhost", port, "\n");

		DataStream<Model> resultStream = AsyncDataStream.unorderedWait(stream, new FeedStreamEnrichment(),
				120, TimeUnit.SECONDS, 100000);
		
		// init sink utils
		ESSinkUtils esSinkUtils = new ESSinkUtils();
		
		resultStream.addSink(new ElasticsearchSink<>(esSinkUtils.createConfigMap(), esSinkUtils.createSocketAddresses(), new ElasticsearchSinkFunction<Model>() {
		    
			/**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			public IndexRequest createIndexRequest(Model element) {
				Gson gson = new Gson();
				return Requests.indexRequest()
		                .index(Constants.INDEX_NAME)
		                .type(Constants.TYPE_NAME)
		                .source(gson.toJson(element));
		    }
		    
		    @Override
		    public void process(Model element, RuntimeContext ctx, RequestIndexer indexer) {
		        indexer.add(createIndexRequest(element));
		    }
		}));

		env.execute("Async Stream Enrichment and Indexing");
	}
}
