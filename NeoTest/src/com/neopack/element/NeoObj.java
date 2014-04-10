package com.neopack.element;

public abstract class NeoObj {

	/**
	 * true for node
	 * false for relation
	 */
	private boolean NodeOrRelation;
	private int number;

	public boolean isNodeOrRelation() {
		return NodeOrRelation;
	}

	public void setNodeOrRelation(boolean nodeOrRelation) {
		NodeOrRelation = nodeOrRelation;
	}

	public int getNumber() {
		return number;
	}

	public void setNumber(int number) {
		this.number = number;
	}
}
