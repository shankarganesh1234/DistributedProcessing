package com.flink.main;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

/**
 * Transforms IN to OUT
 * @author shankarganesh
 *
 */
public class Transformer {

	/**
	 * Transform input to Model
	 * 
	 * @param input
	 * @return
	 */
	public Model transform(JsonParser parser, String input) {
		JsonObject o = parser.parse(input).getAsJsonArray().get(0).getAsJsonObject();
		return new Model(o.get("id").getAsString(), o.get("name").getAsString(), o.get("symbol").getAsString(),
				o.get("rank").getAsString(), o.get("price_usd").getAsString());
	}
}
