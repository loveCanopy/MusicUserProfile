package com.baidumusic.evan;

import java.util.ArrayList;
import java.util.List;

public class Node {

	private String text;
	private String name;
	private String parentId;

	Node(String parentId) {
		this.parentId = parentId;
	}

	private List<Node> childList = new ArrayList<>();

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	@Override
	public int hashCode() {
		return super.hashCode();
	}

	public List<Node> getChildList() {
		return childList;
	}

	public void setChildList(List<Node> childList) {
		this.childList = childList;
	}

	public static Node getInitNode() {
		return null;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name.replaceAll("/| |\t", "");
	}

	@Override
	public String toString() {
		return "Node{" + "text='" + text + '\'' + ", name='" + name + '\'' + ", childList=" + childList + '}';
	}

	public String getParentId() {
		return parentId;
	}

	public void setParentId(String parentId) {
		this.parentId = parentId;
	}
}