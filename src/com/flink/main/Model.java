package com.flink.main;

public class Model {

	private String id;
	private String name;
	private String symbol;
	private String rank;
	private String price_usd;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getSymbol() {
		return symbol;
	}

	public void setSymbol(String symbol) {
		this.symbol = symbol;
	}

	public String getRank() {
		return rank;
	}

	public void setRank(String rank) {
		this.rank = rank;
	}

	public String getPrice_usd() {
		return price_usd;
	}

	public void setPrice_usd(String price_usd) {
		this.price_usd = price_usd;
	}

	public Model() {

	}

	public Model(String id, String name, String symbol, String rank, String price_usd) {
		super();
		this.id = id;
		this.name = name;
		this.symbol = symbol;
		this.rank = rank;
		this.price_usd = price_usd;
	}

	@Override
	public String toString() {
		return "Model [id=" + id + ", name=" + name + ", symbol=" + symbol + ", rank=" + rank + ", price_usd="
				+ price_usd + "]";
	}
}
