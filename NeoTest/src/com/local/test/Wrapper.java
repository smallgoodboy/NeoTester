package com.local.test;

import java.util.Random;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;



public class Wrapper {
	GraphDatabaseService graphDb;
	Transaction tx;
	String DB_PATH  = "E:\\neo4jdb";
	
	int operatorCounter = 0;
	int operatorMax = 10000;
	
	public static enum RelTypes implements RelationshipType
	{
	    KNOWS
	}
	
	public Wrapper(){
		initDBFile();
	}
	
	public void initDBFile(){
		graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( DB_PATH );
		tx = graphDb.beginTx();
	}

	public void createNode(String labelString, String nodePropertyKey, Object nodePropertyValue){
		Label label = DynamicLabel.label(labelString);
		Node node = graphDb.createNode(label);
		node.setProperty(nodePropertyKey, nodePropertyValue);
	}
	
	public void createRelation(String node1Label, String node1Key, Object node1Value,
			String node2Label, String node2Key, Object node2Value,
			RelationshipType relationType, String relationKey, Object relationValue){
		Node node1, node2;
		Relationship relationship;
		Label label = DynamicLabel.label(node1Label);
		ResourceIterator<Node> node1s = graphDb.findNodesByLabelAndProperty(label, node1Key, node1Value).iterator();
		label = DynamicLabel.label(node2Label);
		while(node1s.hasNext()){
			node1 = node1s.next();
			ResourceIterator<Node> node2s = graphDb.findNodesByLabelAndProperty(label, node2Key, node2Value).iterator();
			while(node2s.hasNext()){
				node2 = node2s.next();
				relationship = node1.createRelationshipTo(node2, relationType);
				relationship.setProperty(relationKey, relationValue);
			}
		}
	}
	
	public synchronized void recordPlus(){
		operatorCounter++;
		if(operatorCounter >= operatorMax){
			sendToDB();
			operatorCounter = 0;
		}
	}
	
	public void sendToDB(){
		tx.success();
		tx.close();
		tx = graphDb.beginTx();
	}
	
	
	public static void main(String args[]){
		Wrapper w = new Wrapper();
		Random r = new Random();
		for(int i=0;i<100000;i++){
			w.createNode("Node", "foo", i);
			if(i%1000 == 0){
				for(int j=0;j<1000;j++){
					w.createRelation("Node", "foo", r.nextInt(i), "Node", "foo", r.nextInt(i), Wrapper.RelTypes.KNOWS, "RRRR", i+j);
				}
			}
		}
//		w.createNode("Node", "foo", 123);
//		w.createNode("Node", "foo", 1);
//		w.createNode("Node", "foo", 2);
//		w.createNode("Node", "foo", 3);
//		w.createRelation("Node", "foo", 1, "Node", "foo", 2, Wrapper.RelTypes.KNOWS, "RRRR", 12);
		w.tx.success();
		w.tx.close();
	}
}
