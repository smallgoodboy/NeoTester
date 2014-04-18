package com.onlytest;

import java.io.IOException;
import java.util.Random;
import java.util.Vector;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.DeleteMethod;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.shell.util.json.JSONArray;
import org.neo4j.shell.util.json.JSONException;
import org.neo4j.shell.util.json.JSONObject;

public class NeoPacker2 {

	final String SERVER_ROOT_URI = "http://localhost:7474";
	String nodePointUrl = this.SERVER_ROOT_URI + "/db/data/node";
	String relationshipPointUrl = this.SERVER_ROOT_URI + "/db/data/relationship";
	String batchExecPath = SERVER_ROOT_URI+"/db/data/batch";
	HttpClient client = new HttpClient();
	Header mtHeader;
	
	PostMethod createNodePostMethod;
	PostMethod createRelationMethod;
	PostMethod queryNodePostMethod;
	DeleteMethod deleteNodeMethod;
	PutMethod putMethod;
	GetMethod getMethod;
	PostMethod addRelationshipCypherPostMethod;
	PostMethod batchExecPostMethod;
	
	@SuppressWarnings("unused")
	private static enum RelTypes implements RelationshipType
    {
        KNOWS,friend;
    }
	
	public NeoPacker2(){
		initMTHeader();
	}
	
	private void initMTHeader(){
		mtHeader = new Header();
        mtHeader.setName("content-type");
        mtHeader.setValue("application/json");
        mtHeader.setName("accept");
        mtHeader.setValue("application/json");
        
        createNodePostMethod = new PostMethod();
        createNodePostMethod.addRequestHeader(mtHeader);
//        
        createRelationMethod = new PostMethod();
        createRelationMethod.addRequestHeader(mtHeader);
//        
        queryNodePostMethod = new PostMethod();
        queryNodePostMethod.addRequestHeader(mtHeader);
        
        deleteNodeMethod = new DeleteMethod();
        deleteNodeMethod.addRequestHeader(mtHeader);
        
        getMethod = new GetMethod();
        getMethod.addRequestHeader(mtHeader);
        
        addRelationshipCypherPostMethod = new PostMethod();
        addRelationshipCypherPostMethod.addRequestHeader(mtHeader);
        
        batchExecPostMethod = new PostMethod();
        batchExecPostMethod.addRequestHeader(mtHeader);
	}
	
	/**
     * creates an empty node and returns its URI
     * @param
     * {
  		"foo" : "bar"
		}
     * @return
     */
    public int createNode(String nodeProperty_json){
    	int satus = 0;
        try{
            /**
             * set json payload
             */
            StringRequestEntity requestEntity = new StringRequestEntity(nodeProperty_json,
                                                                        "application/json",
                                                                        "UTF-8");
            createNodePostMethod.setPath(this.nodePointUrl);
            createNodePostMethod.addRequestHeader(mtHeader);
            createNodePostMethod.setRequestEntity(requestEntity);
            satus = client.executeMethod(createNodePostMethod);

            return satus;
        }catch(Exception e){
        	e.printStackTrace();
        }
        return satus;
    }
    
    /**
     * adds a relationship between the target and source node
     * EX.   relationAttributes = "{ \"married\" : \"yes\",\"since\" : \"2005\" }";
        String relationShipURI = neo4jHello.addRelationship("http://localhost:7474/db/data/node/1342",
                                                            "http://localhost:7474/db/data/node/1331",
                                                            "friend",
                                                            relationAttributes);
     * @param startNodeURI
     * @param endNodeURI
     * @param relationshipType
     * @param jsonAttributes
     * @return
     */
    public int addRelationship(int startNodeNumber,
                                   int endNodeNumber,
                                   String relationshipType,
                                   String jsonAttributes){
    	//HttpClient client = new HttpClient();
    	int satus = 0;
        try{
        	//PostMethod createRelationMethod = new PostMethod();
            String fromUrl = this.nodePointUrl + "/" + startNodeNumber + "/relationships";

            String relationshipJson = generateJsonRelationship( endNodeNumber,
                                                                relationshipType,
                                                                jsonAttributes );
            createRelationMethod.setPath(fromUrl);

            /**
             * set json payload
             */
            StringRequestEntity requestEntity = new StringRequestEntity(relationshipJson,
                                                                        "application/json",
                                                                        "UTF-8");
            System.out.println(requestEntity.getContent());
            createRelationMethod.setRequestEntity(requestEntity);
            satus = client.executeMethod(createRelationMethod);

            //createRelationMethod.releaseConnection();
        }catch(Exception e){
             e.printStackTrace();
        }

        return satus;

    }
    
    public int addRelationshipCypher(int startNodeNumber,
            int endNodeNumber,
            String relationshipType,
            String jsonAttributes){
		int satus = 0;
			try{
	            String nodePointUrl = this.SERVER_ROOT_URI + "/db/data/cypher";
	            addRelationshipCypherPostMethod.setPath(nodePointUrl);

	            
	            /**
	             * set json payload
	             */
	            StringRequestEntity requestEntity = new StringRequestEntity("{\"query\" : \"MATCH (a),(b) WHERE a.id='" + startNodeNumber + "'AND b.id='" + endNodeNumber + "' CREATE (a)-[r:RELTYPE]->(b) RETURN r\"}",
	                                                                        "application/json",
	                                                                        "UTF-8");
	            //System.out.println(requestEntity.getContent());
	            addRelationshipCypherPostMethod.setRequestEntity(requestEntity);
	            satus = client.executeMethod(addRelationshipCypherPostMethod);
//	            output = queryNodePostMethod.getResponseBodyAsString( );


	            
//				String fromUrl = this.nodePointUrl + "/" + startNodeNumber + "/relationships";
//				
//				String relationshipJson = generateJsonRelationship( endNodeNumber,
//				                                         relationshipType,
//				                                         jsonAttributes );
//				createRelationMethod.setPath(fromUrl);
//				
//				/**
//				* set json payload
//				*/
//				StringRequestEntity requestEntity = new StringRequestEntity(relationshipJson,
//				                                                 "application/json",
//				                                                 "UTF-8");
//				createRelationMethod.setRequestEntity(requestEntity);
//				satus = client.executeMethod(createRelationMethod);
			
			}catch(Exception e){
				e.printStackTrace();
			}
		
		return satus;
		
		}
    
    /**
     * generates the json payload which is to passed to relationship url
     * @param endNodeURL
     * @param relationshipType
     * @param jsonAttributes
     * @return
     */
    private String generateJsonRelationship(int endNodeNumber,
                                            String relationshipType,
                                            String ... jsonAttributes) {
        StringBuilder sb = new StringBuilder();
        sb.append("{ \"to\" : \"");
        sb.append(this.nodePointUrl + "/" + endNodeNumber);
        sb.append("\", ");

        sb.append("\"type\" : \"");
        sb.append(relationshipType);
        if(jsonAttributes == null || jsonAttributes.length < 1) {
            sb.append("\"");
        } else {
            sb.append("\", \"data\" : ");
            for(int i = 0; i < jsonAttributes.length; i++) {
                sb.append(jsonAttributes[i]);
                if(i < jsonAttributes.length -1) { // Miss off the final comma
                    sb.append(", ");
                }
            }
        }

        sb.append(" }");
        return sb.toString();
    }
    
    /**
     * Use node number to get other info of the node.
     * @param nodeNumber
     * @return
     */
    public Node getNode(int nodeNumber){
    	Node node = new Node();
    	@SuppressWarnings("unused")
		int status;
    	node.setSelfLoaction(this.nodePointUrl + "/" + nodeNumber);
    	String output;
		try {
			getMethod.setPath(this.nodePointUrl + "/" + nodeNumber +"/relationships/out");
			status = client.executeMethod(getMethod);
			output = getMethod.getResponseBodyAsString( );
			node.setOutgoingRelations(resolveJSONNodeRelation("self", output));
			
			getMethod.setPath(this.nodePointUrl + "/" + nodeNumber +"/relationships/in");
			status = client.executeMethod(getMethod);
			output = getMethod.getResponseBodyAsString( );
			node.setIngoingRelations(resolveJSONNodeRelation("self", output));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (JSONException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	return node;
    }


	public int deleteNode(int nodeNumber){
		int status = 0;
	    try{
	    	checkIfRelationDeleted(nodeNumber);
	        String nodePointUrl = this.nodePointUrl + "/"+ nodeNumber;
	        deleteNodeMethod.setPath(nodePointUrl);

	        status = client.executeMethod(deleteNodeMethod);

	    }catch(Exception e){
	    	e.printStackTrace();
	    }
	
	    return status;
	}
	
	private int checkIfRelationDeleted(int nodeNumber){
		Node node = getNode(nodeNumber);
		int status = 0;
		if(node.getIngoingRelations().size() != 0){
			for(int i:node.getIngoingRelations()){
				status |= deleteRelationship(i);
			}
		}
		if(node.getOutgoingRelations().size() != 0){
			for(int i:node.getOutgoingRelations()){
				status |= deleteRelationship(i);
			}
		}
		return status;
	}
	
	/**
	 * Use cypher to get nodes number.
	 * @param key
	 * @param value
	 * @return
	 */
	public Vector<Integer> queryNode(String key, String value){
		String output = null;
    	try{
            String nodePointUrl = this.SERVER_ROOT_URI + "/db/data/cypher";
            queryNodePostMethod.setPath(nodePointUrl);

            
            /**
             * set json payload
             */
            StringRequestEntity requestEntity = new StringRequestEntity("{\"query\" : \"MATCH (n) optional match (n)-[r]-() RETURN n,r limit 10;\"}",
                                                                        "application/json",
                                                                        "UTF-8");
            //System.out.println(requestEntity.getContent());
            queryNodePostMethod.setRequestEntity(requestEntity);
            @SuppressWarnings("unused")
			int satus = client.executeMethod(queryNodePostMethod);
            output = queryNodePostMethod.getResponseBodyAsString( );

            System.out.println(output);
            return resolveJSONToNodes(output);
    	}catch(Exception e){
            e.printStackTrace();
       }
    	return new Vector<Integer>();
	}
	
	public void batchExecREST(){
		batchExecPostMethod.setPath(batchExecPath);
		String content = "["
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
		try {
			System.out.println(content);
			StringRequestEntity requestEntity = new StringRequestEntity(content,
			        "application/json",
			        "UTF-8");
			batchExecPostMethod.setRequestEntity(requestEntity);
            int satus = client.executeMethod(batchExecPostMethod);
            System.out.println(satus);
            System.out.println(batchExecPostMethod.getResponseBodyAsString());
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public Vector<Integer> resolveJSONToNodes(String json) throws JSONException{
		Vector<Integer> tempList = new Vector<Integer>();
		JSONObject tempJSONObject;
		String tempSelfPath;
		String tempSelfPathSplit[];
		if(json.length()<=10){
			return tempList ;
		}
		JSONObject jsonObject = new JSONObject(json);
		JSONArray jsonArray = jsonObject.getJSONArray("data");
		JSONArray tempJSONArray;
		for(int i=0;i<jsonArray.length();i++){
			tempJSONArray = jsonArray.getJSONArray(i);
			tempJSONObject = tempJSONArray.getJSONObject(0);
			tempSelfPath = (String)tempJSONObject.get("self");
			tempSelfPathSplit = tempSelfPath.split("/");
			tempList.add((Integer.valueOf(tempSelfPathSplit[tempSelfPathSplit.length-1])));
		}

		return tempList;
		//jsonObject.get(key);
	}
	
	public int deleteRelationship(int relationshipNumber){
		int status = 0;
	    try{
	        String relationshipPointUrl = this.relationshipPointUrl + "/"+ relationshipNumber;
	        deleteNodeMethod.setPath(relationshipPointUrl);

	        status = client.executeMethod(deleteNodeMethod);

	    }catch(Exception e){
	    	e.printStackTrace();
	    }
	
	    return status;
	}
	
	
	
	public Vector<Integer> resolveJSONNodeRelation(String key, String json) throws JSONException{
		Vector<Integer> tempList = new Vector<Integer>();
		JSONObject tempJSONObject;
		String tempSelfPath;
		String tempSelfPathSplit[];
		json = "{nodes:["+json.substring(1,json.length()-2)+"]}";
		JSONObject jsonObject = new JSONObject(json);
		JSONArray jsonArray = jsonObject.getJSONArray("nodes");
		for(int i=0;i<jsonArray.length();i++){
			tempJSONObject = jsonArray.getJSONObject(i);
			tempSelfPath = (String)tempJSONObject.get(key);
			tempSelfPathSplit = tempSelfPath.split("/");
			tempList.add(Integer.valueOf(tempSelfPathSplit[tempSelfPathSplit.length-1]));
			
		}
		return tempList;
		//jsonObject.get(key);
	}
	
	
	public static void main2(String args[]){
		NeoPacker2 np = new NeoPacker2();
//		np.createNode("{\"name\":\"haha\"}");
//		np.createNode("{\"name\":\"hahass\"}");
//		np.addRelationship(1435, 1434, "frr", "{ \"married\" : \"yes\",\"since\" : \"2005\" }");
//		np.addRelationship(1441, 1442, "frr", "{ \"married\" : \"yes\",\"since\" : \"2005\" }");
		int start,end;
		Random r= new Random();
		for(int i=0;i<200;i++){
			np.createNode("{\"id\":\""+String.valueOf(i)+"\"}");
			if(i%20 == 0)
				System.out.println(i);
		}
		for(int i=0;i<1000;i++){
			while(
			(start = r.nextInt(200)) ==
			(end = r.nextInt(200))){}
//			if(np.queryNode("id",String.valueOf(start)).size()==0){
//				np.createNode("{\"id\":\""+String.valueOf(start)+"\"}");
//			}
//			if(np.queryNode("id",String.valueOf(end)).size()==0){
//				np.createNode("{\"id\":\""+String.valueOf(end)+"\"}");
//			}
			np.addRelationshipCypher(start,end,null,null);
//			int a = np.queryNode("id", String.valueOf(start)).firstElement();
//			int b =np.queryNode("id", String.valueOf(end)).firstElement();
//			np.addRelationship(a, b, "frr", "{ \"married\" : \"yes\",\"since\" : \"2005\" }");
			
			if(i%200 == 0)
			System.out.println(i);
		}
		Vector<Integer> v = np.queryNode("name","haha4");
		for(int i:v){
			System.out.println(i);
		}
//		np.deleteNode(1436);
//		np.deleteNode(1437);
		//np.deleteNode(1423);
	}
	
	public static void main(String args[]){
		NeoPacker2 np = new NeoPacker2();
		np.queryNode(null, null);
	}
	
}