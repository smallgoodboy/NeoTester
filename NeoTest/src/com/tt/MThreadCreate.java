package com.tt;

import java.util.Random;
import java.util.Vector;


public class MThreadCreate {
	NeoPacker2 np = new NeoPacker2();
	Vector<Thread> nodeThreads = new Vector<Thread>();
	Vector<Thread> relationThreads = new Vector<Thread>();
	int nodeCount;
	int totalNodeAmount;
	int relationCount;
	int totalRelationAmount;
	
	private synchronized int getNodeCount(){
		int abc = nodeCount;
		nodeCount++;
		if(nodeCount%10 == 0){
			System.out.println(nodeCount);
		}
		return abc;
	}
	
	private synchronized int getRelationCount(){
		int abc = relationCount;
		relationCount++;
		if(relationCount%10 == 0){
			System.out.println(relationCount);
		}
		return abc;
	}
	
	public void createNodes(int nodeAmount, int threadAmount){
		nodeCount = 0;
		totalNodeAmount = nodeAmount;
		nodeThreads = new Vector<Thread>();
		for(int i=0;i<threadAmount;i++){
			Thread createNodesThread = new Thread() {
				@Override
				public void run() {
					for(;;){
						if(nodeCount>=(totalNodeAmount-1)){
							return;
						}
						np.createNode("{\"id\":\""+String.valueOf(getNodeCount())+"\"}");
					}
				}
			};
			createNodesThread.start();
			nodeThreads.add(createNodesThread);
		}
		
	}
	
	public void createRelationship(int nodeAmount, int relationAmount,int threadAmount){
		relationCount = 0;
		totalRelationAmount = relationAmount;
		totalNodeAmount = nodeAmount;
		relationThreads = new Vector<Thread>();
		for(int i=0;i<threadAmount;i++){
			Thread createRelationThread = new Thread() {
				@Override
				public void run() {
					NeoPacker2 np = new NeoPacker2();
					for(;;){
						if(relationCount>=(totalRelationAmount-1)){
							return;
						}
						Random r = new Random();
						int start, end;
						while(
								(start = r.nextInt(totalNodeAmount)) ==
								(end = r.nextInt(totalNodeAmount))){}
						np.addRelationship(start,end,null,null);
						getRelationCount();
					}
				}
			};
			createRelationThread.start();
			relationThreads.add(createRelationThread);
		}
	}
	
	public void awaitThreads(Vector<Thread>threads){
		for(Thread t:threads){
			if(t.isAlive()){
				try {
					t.join();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}
	
	public void awaitNodeCreate(){
		awaitThreads(nodeThreads);
	}
	
	public void awaitRelationCreate(){
		awaitThreads(relationThreads);
	}
	
	public static void main(String args[]){
		MThreadCreate mtc = new MThreadCreate();
		mtc.createNodes(100, 3);
		mtc.awaitNodeCreate();
		mtc.createRelationship(100, 200, 3);
	}
}
