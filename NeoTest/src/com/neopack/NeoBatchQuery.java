package com.neopack;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import scala.util.Random;

public class NeoBatchQuery {

	
	String batchQueryString;
	/**
	 * Temp store query string
	 * ready to send to db.
	 * Write and read is mutual exclusion。
	 */
	Queue<String> queryRecordQueue = new ArrayBlockingQueue<String>(10000);
	/**
	 * Class to send request to db.
	 */
	NeoRestConnection neoDBConnector = new NeoRestConnection();
	/**
	 * How many request string in @queryRecordQueue
	 */
	int queryCounter = 0;
	/**
	 * If @queryCounter > @querySendAmount,
	 * we will send data in @queryRecordQueue to db.
	 */
	private int querySendAmount = 1000;
	
	/**
	 * {\"method\":\"POST\",\"to\":\"/node\",\"body\":{\"nom\":\"organisation\",\"kaka\":\"123\"}}
	 * @param nodeProperty property of node 
	 * 		EX. {\"nom\":\"organisation\",\"kaka\":\"123\"}
	 */
	public void createNode(String nodeProperty){
		String queryString = "{\"method\":\"POST\",\"to\":\"/node\",\"body\":" + nodeProperty + "}";
		queryRecordQueue.offer(queryString);
		queryCounterIncrease();
	}
	public void createNodeCypher(String type, String nodeProperty){
		String queryString = "{\"method\":\"POST\",\"to\":\"/cypher\",\"body\":{\"query\" : \"CREATE (n:"+ type + nodeProperty + ") return n\"}}";
//		System.out.println(queryString);
		queryRecordQueue.offer(queryString);
		queryCounterIncrease();
	}
	
	/**
	 * {"method": "POST",
	 * 	"to": "/cypher",
	 * 	"id": 1,
	 * 	"body": {"query" : "MATCH a,b WHERE a.kaka=123 AND b.nom='organisation' CREATE b-[r:est]->a RETURN a, b, r"}}
	 * @param aCondition
	 * @param bCondition
	 */
	public int createRelation(String nodeLabel, Map<Object,Object>aCondition, Map<Object,Object>bCondition, String relationType, String relationProperty){
		String queryStringResult = "{\"method\": \"POST\",\"to\": \"/cypher\",\"body\": {\"query\" : \"";
		String cypher = createCypherCreateRelation(nodeLabel, aCondition, bCondition, relationType, relationProperty);
		if(cypher == null){
			return -1;
		}
		queryStringResult += cypher;
		queryStringResult += "\"}}";
//		System.out.println(queryStringResult);
		queryRecordQueue.offer(queryStringResult);
		queryCounterIncrease();
		return 1;
	}

	/**
	 * MATCH a,b WHERE a.kaka=123 AND b.nom='organisation' CREATE b-[r:est]->a RETURN a, b, r
	 * @param aCondition
	 * @param bCondition
	 * @param relationProperty $:Group{property}$ 
	 * @param relationType cannot be null!!
	 * @return
	 */
	private String createCypherCreateRelation(String nodeLabel, Map<Object, Object> aCondition, Map<Object,Object>bCondition, String relationType, String relationProperty) {
		if(relationType == null || relationType.length() == 0){
			return null;
		}
		String queryStringResult = "MATCH ";
		String queryString = "";
		String queryStringA = "";
		String queryStringB = "";
		for(Object a:aCondition.keySet()){
			queryStringA += ("@"+ a + ":");
			Object aa = aCondition.get(a);
			if(aa.getClass().toString().toLowerCase().contains("string")){
				queryStringA += "'" + aa + "'";
			}else if(aa.getClass().toString().toLowerCase().contains("integer")){
				queryStringA += aa;
			}
		}
		queryStringA = "(a:"+ nodeLabel +"{" +queryStringA.substring(1)+"})";
		for(Object a:bCondition.keySet()){
			queryStringB += ("@"+ a + ":");
			Object aa = bCondition.get(a);
			if(aa.getClass().toString().toLowerCase().contains("string")){
				queryStringB += "'" + aa + "'";
			}else if(aa.getClass().toString().toLowerCase().contains("integer")){
				queryStringB += aa;
			}
		}
		queryStringB = "(b:"+ nodeLabel +"{ "+queryStringB.substring(1)+"})";
//		if(queryString!=null && queryString.contains("@")){
//			queryString = queryString.substring(1);
//			queryString = queryString.replaceAll("@", " AND ");
//		}
		queryString += (queryStringA + "," + queryStringB);
		if(relationProperty!=null && relationProperty.contains("{")){
			queryString += " CREATE a-[r"+ ":" + relationType + relationProperty +"]->b";
		}else{
			queryString += " CREATE a-[r"+ ":" + relationType +"]->b";
		}
//		System.out.println(queryString);
		return queryStringResult + queryString;
	}
	
	/**
	 * 
	 * @return
	 */
	private int queryCounterIncrease(){
		queryCounter++;
		if(queryCounter>=querySendAmount){
			batchQueryNeoDB();
//			System.out.println("123");
		}
		return queryCounter;
	}
	
	/**
	 * 
	 * @return:
	 * "["
		    +"{\"method\":\"POST\",\"to\":\"/node\",\"body\":{\"nom\":\"organisation\",\"kaka\":123}},"
		    +"{\"method\":\"POST\",\"to\":\"/node\",\"body\":{\"nom\":\"etablissement\"}},"
		    +"{"
		    +    "\"method\": \"POST\","
		    +    "\"to\": \"/cypher\","
		    +    "\"id\": 1,"
		    +    "\"body\": {"
		    +      "\"query\" : \"MATCH a,b WHERE a.kaka=123 AND b.nom='organisation' CREATE b-[r:est]->a RETURN a, b, r\","
		    +      "\"params\" : {"
		    +        "\"aVal\" : \"etablissement\","
		    +        "\"bVal\" : \"organisation\""
		    +      "}}}]";
	 */
	public String sendDBSentense(){
		String batchString = "";
		if(queryRecordQueue.size() == 0){
			return null;
		}
		while(queryRecordQueue.size()>0){
			String a = queryRecordQueue.poll();
			batchString += ("," + a);
		}
		batchString = "[" + batchString.substring(1) + "]";
		return batchString;
	}
	
	public void batchQueryNeoDB(){
		String batchString = sendDBSentense();
		//System.out.println(batchString);
		if(batchString != null && batchString.length()>1){
			neoDBConnector.batchExecREST(batchString);
		}
		queryCounter = 0;
	}
	
	public void endTransmation(){
		batchQueryNeoDB();
	}

	public int getQuerySendAmount() {
		return querySendAmount;
	}

	public void setQuerySendAmount(int querySendAmount) {
		this.querySendAmount = querySendAmount;
	}
	
	public void deleteAll(){
		String deleteString = "match n optional match (n)-[r]->() delete n,r";
		neoDBConnector.execCypher(deleteString);
	}
	
	public void deleteNode(String nodeLabel, String nodeProperty){
		String queryStringResult = "{\"method\": \"POST\",\"to\": \"/cypher\",\"body\": {\"query\" : \"";
		String resString = "match (a:"+ nodeLabel + nodeProperty +") optional match (a)-[r]-() delete a,r";
		queryStringResult += (resString + "\"}}");
		queryRecordQueue.offer(queryStringResult);
		queryCounterIncrease();
//		return queryStringResult;
	}
	
	public static void main2(String args[]){
		NeoBatchQuery nbq = new NeoBatchQuery();
		Random r = new Random();
		long startMili=System.currentTimeMillis();
		for(int i=0;i<1000;i++){
			nbq.createNodeCypher("Node", "{foo : 'bar"+i+"'}");
			if(i%1000 == 0){
				System.out.println(i);
			}
		}
//		nbq.createNode("{\"foo\" : \"bar2\"}");
//		nbq.createNode("{\"foo\" : \"bar3\"}");
//		nbq.createNode("{\"foo\" : \"bar4\"}");
		for(int i=0;i<1000;i++){
			Map<Object,Object> a = new HashMap<Object,Object>();
			Map<Object,Object> b = new HashMap<Object,Object>();
			a.put("foo", "bar"+r.nextInt(1000));
			b.put("foo", "bar"+r.nextInt(1000));
			nbq.createRelation("Node", a, b, "caca", "{qwe:"+i+"}");
			if(i%100 == 0){
				System.out.println(i);
			}
		}
		nbq.endTransmation();
		long endMili=System.currentTimeMillis();
		System.out.println("总耗时为："+(endMili-startMili)+"毫秒");
	}
	
	public static void main(String args[]) throws IOException{
		NeoBatchQuery nbq = new NeoBatchQuery();
		Random r = new Random();
		//int temp[] = {500000,1000000};
		FileOutputStream out=new FileOutputStream("D:/neorecord.txt");
		PrintStream p=new PrintStream(out);
		for(int j=0;j<1;j++){
			long startMili=System.currentTimeMillis();
			long tempMili = System.currentTimeMillis();
			for(int i=2000000;i<3000000;i++){
				nbq.createNodeCypher("Node", "{foo : '"+i+"'}");
				
				if(i%10000 == 0 && i!= 0){
					for(int k=0;k<10000;k++){
						Map<Object,Object> a = new HashMap<Object,Object>();
						Map<Object,Object> b = new HashMap<Object,Object>();
						a.put("foo", ""+r.nextInt(i));
						b.put("foo", ""+r.nextInt(i));
						nbq.createRelation("Node", a, b, "caca", "{qwe:"+(i+k)+"}");
					}
		             p.print(i+"  ");
		             p.print((System.currentTimeMillis()-tempMili)+"  ");
		             p.println((System.currentTimeMillis()-startMili));
					System.out.println(i);
					System.out.println("Interval cost::"+(System.currentTimeMillis()-tempMili) +"ms");
					System.out.println("Total:"+(System.currentTimeMillis()-startMili) +"ms");
					tempMili = System.currentTimeMillis();
				}
			}
			nbq.endTransmation();
			p.close();
			out.close();
	//		
	//		nbq.createNode("{\"foo\" : \"bar4\"}");
	//		for(int i=0;i<5000;i++){
	//			Map<Object,Object> a = new HashMap<Object,Object>();
	//			Map<Object,Object> b = new HashMap<Object,Object>();
	//			a.put("foo", "bar"+r.nextInt(100000));
	//			b.put("foo", "bar"+r.nextInt(100000));
	//			nbq.createRelation("Node", a, b, "caca", "{qwe:123}");
	//			if(i%100 == 0){
	//				System.out.println(i);
	//			}
	//		}
			long endMili=System.currentTimeMillis();
			System.out.println(" 总耗时为："+(endMili-startMili)+"毫秒");
			
		}
	}
}
