package com.neopack;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;

public class NeoRestConnection {

	final String SERVER_ROOT_URI = "http://localhost:7474";
	String NODE_REQUEST_URI = SERVER_ROOT_URI + "/db/data/node";
	String RELATION_REQUEST_URI = SERVER_ROOT_URI + "/db/data/relationship";
	String BTACH_REQUEST_URI = SERVER_ROOT_URI+"/db/data/batch";
	
	HttpClient client = new HttpClient();
	Header mtHeader;
	
	public NeoRestConnection(){
		initMTHeader();
	}
	
	private void initMTHeader(){
		mtHeader = new Header();
        mtHeader.setName("content-type");
        mtHeader.setValue("application/json");
        mtHeader.setName("accept");
        mtHeader.setValue("application/json");
	}
	
	/**
     * creates an empty node and returns its URI
     * @param {"foo" : "bar"}
     * @return
     */
    public int createNode(String nodeProperty_json){
    	int satus = 0;
    	PostMethod createNodePostMethod = new PostMethod();
        createNodePostMethod.addRequestHeader(mtHeader);
        try{
            /**
             * set json payload
             */
            StringRequestEntity requestEntity = new StringRequestEntity(nodeProperty_json,
                                                                        "application/json",
                                                                        "UTF-8");
            createNodePostMethod.setPath(NODE_REQUEST_URI);
            createNodePostMethod.addRequestHeader(mtHeader);
            createNodePostMethod.setRequestEntity(requestEntity);
            satus = client.executeMethod(createNodePostMethod);
            createNodePostMethod.releaseConnection();

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
    public int createRelationship(int startNodeNumber,
                                   int endNodeNumber,
                                   String relationshipType,
                                   String jsonAttributes){
    	//HttpClient client = new HttpClient();
    	int satus = 0;
    	PostMethod createRelationPostMethod = new PostMethod();
    	createRelationPostMethod.addRequestHeader(mtHeader);
        try{
        	//PostMethod createRelationMethod = new PostMethod();
            String fromUrl = this.NODE_REQUEST_URI + "/" + startNodeNumber + "/relationships";

            String relationshipJson = generateJsonRelationship( endNodeNumber,
                                                                relationshipType,
                                                                jsonAttributes );
            createRelationPostMethod.setPath(fromUrl);

            /**
             * set json payload
             */
            StringRequestEntity requestEntity = new StringRequestEntity(relationshipJson,
                                                                        "application/json",
                                                                        "UTF-8");
            System.out.println(requestEntity.getContent());
            createRelationPostMethod.setRequestEntity(requestEntity);
            satus = client.executeMethod(createRelationPostMethod);
            createRelationPostMethod.releaseConnection();

            //createRelationMethod.releaseConnection();
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
        sb.append(this.NODE_REQUEST_URI + "/" + endNodeNumber);
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
     * 
     * @param batchString:
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
    @SuppressWarnings("finally")
	public int batchExecREST(String batchString){
    	PostMethod batchExecPostMethod = new PostMethod();
    	batchExecPostMethod.addRequestHeader(mtHeader);
		batchExecPostMethod.setPath(BTACH_REQUEST_URI);
		int status = 0;

		try {
//			System.out.println(content);
			StringRequestEntity requestEntity = new StringRequestEntity(batchString,
			        "application/json",
			        "UTF-8");
			batchExecPostMethod.setRequestEntity(requestEntity);
            status = client.executeMethod(batchExecPostMethod);
            batchExecPostMethod.releaseConnection();
//            System.out.println(satus);
//            System.out.println(batchExecPostMethod.getResponseBodyAsString());
		} catch (Exception e) {
			e.printStackTrace();
		}finally{
			return status;
		}
	}
    
    
    
    
    public static void main(String args[]){
    	NeoRestConnection nrc = new NeoRestConnection();
    	nrc.createNode("{\"foo\" : \"bar\"}");
    	nrc.createNode("{\"kaka\" : 123}");
    	nrc.createRelationship(30 , 31, "fror", "{ \"married\" : \"yes\",\"since\" : \"2005\" }");
    }
}
