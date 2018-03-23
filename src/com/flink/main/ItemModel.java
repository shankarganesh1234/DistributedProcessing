package com.flink.main;

public class ItemModel {

	private String id;
	private String title;
	private Integer categoryId;

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getTitle() {
		return title;
	}

	public void setTitle(String title) {
		this.title = title;
	}

	@Override
	public String toString() {
		return "ItemModel [id=" + id + ", title=" + title + ", categoryId=" + categoryId + "]";
	}

	public ItemModel(String id, String title, Integer categoryId) {
		super();
		this.id = id;
		this.title = title;
		this.categoryId = categoryId;
	}

	public ItemModel() {

	}

	public Integer getCategoryId() {
		return categoryId;
	}

	public void setCategoryId(Integer categoryId) {
		this.categoryId = categoryId;
	}
}
