package com.neopack.element;

import java.util.HashMap;
import java.util.Map;

public class Relationship extends NeoObj{

	private String selfLoaction;
	private Node startNode;
	private Node endNode;
	Map<String, String>data = new HashMap<String, String>();
	
	public String getSelfLoaction() {
		return selfLoaction;
	}
	
	public void setSelfLoaction(String selfLoaction) {
		this.selfLoaction = selfLoaction;
	}
	
	public Node getStartNode() {
		return startNode;
	}
	
	public void setStartNode(Node startNode) {
		this.startNode = startNode;
	}
	
	public Node getEndNode() {
		return endNode;
	}
	
	public void setEndNode(Node endNode) {
		this.endNode = endNode;
	}
	
}
