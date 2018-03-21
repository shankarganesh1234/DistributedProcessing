package com.flink.main;

import java.io.IOException;
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

import com.google.gson.JsonParser;
import com.google.gson.JsonSyntaxException;

/**
 * Performs stream enrichment by invoking a web service or db 
 * @author shankarganesh
 *
 */
public class FeedStreamEnrichment extends RichAsyncFunction<String, Model> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private transient CloseableHttpAsyncClient asyncHttpClient;

	@Override
	public void open(Configuration parameters) throws Exception {
		
		// default conn per route for the same endpoint is 2.
		asyncHttpClient = HttpAsyncClients.custom().setMaxConnPerRoute(2000).setMaxConnTotal(5000)
				.setDefaultRequestConfig(RequestConfig.custom().build()).build();
		asyncHttpClient.start();
	}

	@Override
	public void close() throws Exception {
		asyncHttpClient.close();
	}

	@Override
	public void asyncInvoke(String str, AsyncCollector<Model> collector) throws Exception {
		// issue the asynchronous request, receive a future for result
		try {
			JsonParser parser = new JsonParser();
			Transformer transformer = new Transformer();
			HttpGet request1 = new HttpGet(Constants.BASE_ENDPOINT + str);

			asyncHttpClient.execute(request1, new FutureCallback<HttpResponse>() {

				public void completed(final HttpResponse response2) {
					try {
						collector.collect(Collections.singleton(transformer.transform(parser, IOUtils.toString(response2.getEntity().getContent(), "UTF-8"))));
					} catch (JsonSyntaxException | UnsupportedOperationException | IOException e) {
						// TODO Auto-generated catch block
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
