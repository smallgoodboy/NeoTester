package com.neopack;

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
	 * Write and read is mutual exclusion��
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
	private int querySendAmount = 50;
	
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
		if(batchString != null){
			neoDBConnector.batchExecREST(batchString);
		}
		queryCounter = 0;
	}

	public int getQuerySendAmount() {
		return querySendAmount;
	}

	public void setQuerySendAmount(int querySendAmount) {
		this.querySendAmount = querySendAmount;
	}
	
	public static void main(String args[]){
		NeoBatchQuery nbq = new NeoBatchQuery();
		Random r = new Random();
		long startMili=System.currentTimeMillis();
//		for(int i=0;i<100000;i++){
//			nbq.createNodeCypher("Node", "{foo : 'bar"+i+"'}");
//			if(i%1000 == 0){
//				System.out.println(i);
//			}
//			}
//		nbq.createNode("{\"foo\" : \"bar2\"}");
//		nbq.createNode("{\"foo\" : \"bar3\"}");
//		nbq.createNode("{\"foo\" : \"bar4\"}");
		for(int i=0;i<5000;i++){
			Map<Object,Object> a = new HashMap<Object,Object>();
			Map<Object,Object> b = new HashMap<Object,Object>();
			a.put("foo", "bar"+r.nextInt(100000));
			b.put("foo", "bar"+r.nextInt(100000));
			nbq.createRelation("Node", a, b, "caca", "{qwe:123}");
			if(i%100 == 0){
				System.out.println(i);
			}
		}
		long endMili=System.currentTimeMillis();
		System.out.println("�ܺ�ʱΪ��"+(endMili-startMili)+"����");
	}
}
