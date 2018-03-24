package com.flink.enrichment;

import java.util.Collections;

import org.apache.commons.io.IOUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.async.collector.AsyncCollector;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.concurrent.FutureCallback;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;

import com.flink.constants.Constants;
import com.flink.models.ItemModel;
import com.flink.transformer.Transformer;
import com.google.gson.JsonParser;

/**
 * Performs stream enrichment by invoking webservice, performing transformation
 * and collecting results
 * 
 * @author shankarganesh
 *
 */
public class FeedStreamEnrichment extends RichAsyncFunction<String, ItemModel> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private transient CloseableHttpAsyncClient asyncHttpClient;

	@Override
	public void open(Configuration parameters) throws Exception {

		// default conn per route for the same endpoint is 2.
		asyncHttpClient = HttpAsyncClients.custom().setMaxConnPerRoute(100).setMaxConnTotal(200)
				.setDefaultRequestConfig(RequestConfig.custom().build()).build();
		asyncHttpClient.start();
	}

	@Override
	public void close() throws Exception {
		asyncHttpClient.close();
	}

	@Override
	public void asyncInvoke(String str, AsyncCollector<ItemModel> collector) throws Exception {
		// issue the asynchronous request, receive a future for result
		try {
			// init parsers and transformers
			JsonParser parser = new JsonParser();
			Transformer transformer = new Transformer();
			
			// init web service request
			HttpGet request1 = new HttpGet(Constants.ITEM_BASE_ENDPOINT + str);
			request1.addHeader("Authorization", Constants.TOKEN);

			asyncHttpClient.execute(request1, new FutureCallback<HttpResponse>() {

				public void completed(final HttpResponse response2) {
					try {
						if (response2.getStatusLine().getStatusCode() == 200) {
							collector.collect(Collections.singleton(transformer.transformItem(parser,
									IOUtils.toString(response2.getEntity().getContent(), "UTF-8"))));
						}
					} catch (Exception e) {
						e.printStackTrace();
					}
				}

				public void failed(final Exception ex) {
					System.out.println(request1.getRequestLine() + "->" + ex);
				}

				public void cancelled() {
					System.out.println(request1.getRequestLine() + " cancelled");
				}
			});

		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
}
