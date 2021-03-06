package com.local.test;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.graphdb.schema.Schema;



public class Wrapper {
	GraphDatabaseService graphDb;
	Transaction tx;
	String DB_PATH  = "neo4jdb5";
	
	FileOutputStream out;
	PrintStream p;
	
	int operatorCounter = 0;
	int operatorMax = 5000;
	
	public static enum RelTypes implements RelationshipType
	{
	    KNOWS
	}
	
	public Wrapper(){
		initDBFile();
		initLogger();
	}
	
	public void initLogger(){
		try {
			out=new FileOutputStream("neospeed.log");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		p=new PrintStream(out);
	}
	
	public void initDBFile(){
		graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( DB_PATH );
		tx = graphDb.beginTx();
	}

	public void createNode(String labelString, String nodePropertyKey, Object nodePropertyValue){
		Label label = DynamicLabel.label(labelString);
		Node node = graphDb.createNode(label);
		node.setProperty(nodePropertyKey, nodePropertyValue);
		recordPlus();
	}
	
	public void createRelation(String node1Label, String node1Key, Object node1Value,
			String node2Label, String node2Key, Object node2Value,
			RelationshipType relationType, String relationKey, Object relationValue){
		Relationship relationship;
		Label label = DynamicLabel.label(node1Label);
		label = DynamicLabel.label(node2Label);
		for(Node node1:graphDb.findNodesByLabelAndProperty(label, node1Key, node1Value)){
			for(Node node2:graphDb.findNodesByLabelAndProperty(label, node2Key, node2Value)){
				relationship = node1.createRelationshipTo(node2, relationType);
				relationship.setProperty(relationKey, relationValue);
			}
		}
		recordPlus();
	}
	
	public synchronized void recordPlus(){
		operatorCounter++;
		if(operatorCounter >= operatorMax){
			sendToDB();
			operatorCounter = 0;
		}
	}
	long startMili=System.currentTimeMillis();
	long endMili = System.currentTimeMillis();
	
	public void sendToDB(){
		tx.success();
		tx.close();
		System.out.println("Data gen:"+(System.currentTimeMillis()-startMili) +"ms");
		p.print((System.currentTimeMillis()-startMili)+"  ");
		startMili=System.currentTimeMillis();
		tx = graphDb.beginTx();
	}
	
	public void createIndex(String indexLabel, String indexKey){
		Schema schema = graphDb.schema();
		IndexDefinition indexDefinition = schema.indexFor( DynamicLabel.label(indexLabel) )
	            .on(indexKey)
	            .create();
		sendToDB();
		waitForIndexComplete(indexDefinition);
	}
	
	public void waitForIndexComplete(IndexDefinition indexDefinition){
		Schema schema = graphDb.schema();
	    schema.awaitIndexOnline( indexDefinition, 10, TimeUnit.SECONDS );
	}
	
	
	public static void main(String args[]){
		Wrapper w = new Wrapper();
		Random r = new Random();
		long startMili=System.currentTimeMillis();
		long tempMili = System.currentTimeMillis();
//		w.createIndex("Node", "foo");
		for(int i=0;i<500000;i++){
			w.createNode("Node", "foo", i);
			if(i%10000 == 0 && i != 0){
				for(int j=0;j<10000;j++){
					w.createRelation("Node", "foo", r.nextInt(i), "Node", "foo", r.nextInt(i), Wrapper.RelTypes.KNOWS, "RRRR", i+j);
				}
				System.out.println(i);
				System.out.println("Interval cost::"+(System.currentTimeMillis()-tempMili) +"ms");
				System.out.println("Total:"+(System.currentTimeMillis()-startMili) +"ms");
				w.p.println();
				w.p.println(i);
				w.p.println("Interval cost:"+(System.currentTimeMillis()-tempMili) +"ms");
				w.p.println("Total:"+(System.currentTimeMillis()-startMili) +"ms");
				w.p.println();
				tempMili = System.currentTimeMillis();
			}
		}
//		w.createNode("Node", "foo", 123);
//		w.createNode("Node", "foo", 1);
//		w.createNode("Node", "foo", 2);
//		w.createNode("Node", "foo", 3);
//		w.createRelation("Node", "foo", 1, "Node", "foo", 2, Wrapper.RelTypes.KNOWS, "RRRR", 12);
		w.tx.success();
		w.tx.close();
		w.p.close();
	}
}
