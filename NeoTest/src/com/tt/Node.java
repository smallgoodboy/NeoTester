package com.tt;

import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

public class Node {

	private int nodeNumberInDB;
	private String selfLoaction;
	private Vector<Integer> outgoingRelations = new Vector<Integer>();
	private Vector<Integer> ingoingRelations = new Vector<Integer>();
	Map<String, String>data = new HashMap<String, String>();
	
	public String getSelfLoaction() {
		return selfLoaction;
	}
	public void setSelfLoaction(String selfLoaction) {
		this.selfLoaction = selfLoaction;
	}
	public Vector<Integer> getOutgoingRelations() {
		return outgoingRelations;
	}
	public void addOutgoingRelations(int outgoingRelation) {
		getOutgoingRelations().add(outgoingRelation);
	}
	
	public Vector<Integer> getIngoingRelations() {
		return ingoingRelations;
	}
	public void addIngoingRelations(int ingoingRelation) {
		getIngoingRelations().add(ingoingRelation);
	}
	public void setOutgoingRelations(Vector<Integer> outgoingRelations) {
		this.outgoingRelations = outgoingRelations;
	}
	public void setIngoingRelations(Vector<Integer> ingoingRelations) {
		this.ingoingRelations = ingoingRelations;
	}
	public int getNodeNumberInDB() {
		return nodeNumberInDB;
	}
	public void setNodeNumberInDB(int nodeNumberInDB) {
		this.nodeNumberInDB = nodeNumberInDB;
	}
	
	
}
