package com;


/**
 * Licensed to Neo Technology under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Neo Technology licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import java.io.File;
import java.io.IOException;

import org.apache.commons.httpclient.Header;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.PutMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.neo4j.kernel.impl.util.FileUtils;

public class Tester
{
    private static final String DB_PATH = "http://localhost:12345/";

    public String greeting;

    // START SNIPPET: vars
    GraphDatabaseService graphDb;
    Node firstNode;
    Node secondNode;
    Relationship relationship;
    // END SNIPPET: vars

    // START SNIPPET: createReltype
    private static enum RelTypes implements RelationshipType
    {
        KNOWS
    }
    // END SNIPPET: createReltype

    public static void main( final String[] args )
    {
    	Tester hello = new Tester();
        hello.createDb();
//        //hello.removeData();
//        hello.shutDown();
    	//hello.getServerStatus();
//    	hello.createNode();
//    	hello.addProperty(hello.SERVER_ROOT_URI + "/db/data/node" + "/1", "name2", "234");
//    	hello.addRelationship(hello.SERVER_ROOT_URI + "/db/data/node" + "/1", 
//    			hello.SERVER_ROOT_URI + "/db/data/node" + "/3", "test", null);
    }

    void createDb()
    {
        //clearDb();
        // START SNIPPET: startDb
        graphDb = new GraphDatabaseFactory().newEmbeddedDatabase( DB_PATH );
        registerShutdownHook( graphDb );
        // END SNIPPET: startDb

        // START SNIPPET: transaction
        try ( Transaction tx = graphDb.beginTx() )
        {
            // Database operations go here
            // END SNIPPET: transaction
            // START SNIPPET: addData
            firstNode = graphDb.createNode();
            firstNode.setProperty( "message", "Hello, " );
            secondNode = graphDb.createNode();
            secondNode.setProperty( "message", "World!" );

            relationship = firstNode.createRelationshipTo( secondNode, RelTypes.KNOWS );
            relationship.setProperty( "message", "brave Neo4j " );
            // END SNIPPET: addData

            // START SNIPPET: readData
            System.out.print( firstNode.getProperty( "message" ) );
            System.out.print( relationship.getProperty( "message" ) );
            System.out.print( secondNode.getProperty( "message" ) );
            // END SNIPPET: readData

            greeting = ( (String) firstNode.getProperty( "message" ) )
                       + ( (String) relationship.getProperty( "message" ) )
                       + ( (String) secondNode.getProperty( "message" ) );

            // START SNIPPET: transaction
            tx.success();
        }
        // END SNIPPET: transaction
    }

    @SuppressWarnings("unused")
	private void clearDb()
    {
        try
        {
            FileUtils.deleteRecursively( new File( DB_PATH ) );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    void removeData()
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            // START SNIPPET: removingData
            // let's remove the data
            firstNode.getSingleRelationship( RelTypes.KNOWS, Direction.OUTGOING ).delete();
            firstNode.delete();
            secondNode.delete();
            // END SNIPPET: removingData

            tx.success();
        }
    }

    void shutDown()
    {
        System.out.println();
        System.out.println( "Shutting down database ..." );
        // START SNIPPET: shutdownServer
        graphDb.shutdown();
        // END SNIPPET: shutdownServer
    }

    // START SNIPPET: shutdownHook
    private static void registerShutdownHook( final GraphDatabaseService graphDb )
    {
        // Registers a shutdown hook for the Neo4j instance so that it
        // shuts down nicely when the VM exits (even if you "Ctrl-C" the
        // running application).
        Runtime.getRuntime().addShutdownHook( new Thread()
        {
            @Override
            public void run()
            {
                graphDb.shutdown();
            }
        } );
    }
    // END SNIPPET: shutdownHook
    String SERVER_ROOT_URI = "http://localhost:12345";
    
    public int getServerStatus(){
        int status = 500;
        try{
            //SERVER_ROOT_URI = 'http://localhost:7474' 
            String url = SERVER_ROOT_URI;
            HttpClient client = new HttpClient();
            GetMethod mGet =   new GetMethod(url);
            status = client.executeMethod(mGet);
            mGet.releaseConnection( );
        }catch(Exception e){
        System.out.println("Exception in connecting to neo4j : " + e);
        }
     
        return status;
    }
    
    public String createNode(){
        String output = null;
        String location = null;
        try{
            String nodePointUrl = this.SERVER_ROOT_URI + "/db/data/node";
            HttpClient client = new HttpClient();
            PostMethod mPost = new PostMethod(nodePointUrl);
 
            /**
             * set headers
             */
            Header mtHeader = new Header();
            mtHeader.setName("content-type");
            mtHeader.setValue("application/json");
            mtHeader.setName("accept");
            mtHeader.setValue("application/json");
            mPost.addRequestHeader(mtHeader);
 
            /**
             * set json payload
             */
            StringRequestEntity requestEntity = new StringRequestEntity("{}",
                                                                        "application/json",
                                                                        "UTF-8");
            mPost.setRequestEntity(requestEntity);
            int satus = client.executeMethod(mPost);
            output = mPost.getResponseBodyAsString( );
            Header locationHeader =  mPost.getResponseHeader("location");
            location = locationHeader.getValue();
            mPost.releaseConnection( );
            System.out.println("satus : " + satus);
            System.out.println("location : " + location);
            System.out.println("output : " + output);
        }catch(Exception e){
        System.out.println("Exception in creating node in neo4j : " + e);
        }
 
        return location;
    }
    
    
    public void addProperty(String nodeURI,
            String propertyName,
            String propertyValue){
    	String output = null;
	
	try{
		String nodePointUrl = nodeURI + "/properties/" + propertyName;
		HttpClient client = new HttpClient();
		PutMethod mPut = new PutMethod(nodePointUrl);
		
		/**
		* set headers
		*/
		Header mtHeader = new Header();
		mtHeader.setName("content-type");
		mtHeader.setValue("application/json");
		mtHeader.setName("accept");
		mtHeader.setValue("application/json");
		mPut.addRequestHeader(mtHeader);
		
		/**
		* set json payload
		*/
		String jsonString = "\"" + propertyValue + "\"";
		StringRequestEntity requestEntity = new StringRequestEntity(jsonString,
		                                                        "application/json",
		                                                        "UTF-8");
		mPut.setRequestEntity(requestEntity);
		int satus = client.executeMethod(mPut);
		output = mPut.getResponseBodyAsString( );
		
		mPut.releaseConnection( );
		System.out.println("satus : " + satus);
		System.out.println("output : " + output);
	}catch(Exception e){
		System.out.println("Exception in creating node in neo4j : " + e);
	}
	
	}
    
    public String addRelationship(String startNodeURI,
            String endNodeURI,
            String relationshipType,
            String jsonAttributes){
		String output = null;
		String location = null;
		try{
			String fromUrl = startNodeURI + "/relationships";
			System.out.println("from url : " + fromUrl);
			
			String relationshipJson = generateJsonRelationship( endNodeURI,
			                                         relationshipType,
			                                         jsonAttributes );
			
			System.out.println("relationshipJson : " + relationshipJson);
			
			HttpClient client = new HttpClient();
			PostMethod mPost = new PostMethod(fromUrl);
			
			/**
			* set headers
			*/
			Header mtHeader = new Header();
			mtHeader.setName("content-type");
			mtHeader.setValue("application/json");
			mtHeader.setName("accept");
			mtHeader.setValue("application/json");
			mPost.addRequestHeader(mtHeader);
			
			/**
			* set json payload
			*/
			StringRequestEntity requestEntity = new StringRequestEntity(relationshipJson,
			                                                 "application/json",
			                                                 "UTF-8");
			mPost.setRequestEntity(requestEntity);
			int satus = client.executeMethod(mPost);
			output = mPost.getResponseBodyAsString( );
			Header locationHeader =  mPost.getResponseHeader("location");
			location = locationHeader.getValue();
			mPost.releaseConnection( );
			System.out.println("satus : " + satus);
			System.out.println("location : " + location);
			System.out.println("output : " + output);
		}catch(Exception e){
			System.out.println("Exception in creating node in neo4j : " + e);
		}
		
		return location;
	
	}
	
    private String generateJsonRelationship(String endNodeURL,
            String relationshipType,
            String ... jsonAttributes) {
		StringBuilder sb = new StringBuilder();
		sb.append("{ \"to\" : \"");
		sb.append(endNodeURL);
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
    
    @SuppressWarnings("unused")
	private void addPropertyToRelation( String relationshipUri,
            String propertyName,
            String propertyValue ){

		String output = null;
		
		try{
			String relPropUrl = relationshipUri + "/properties";
			HttpClient client = new HttpClient();
			PutMethod mPut = new PutMethod(relPropUrl);
			
			/**
			* set headers
			*/
			Header mtHeader = new Header();
			mtHeader.setName("content-type");
			mtHeader.setValue("application/json");
			mtHeader.setName("accept");
			mtHeader.setValue("application/json");
			mPut.addRequestHeader(mtHeader);
			
			/**
			* set json payload
			*/
			String jsonString = toJsonNameValuePairCollection(propertyName,propertyValue );
			StringRequestEntity requestEntity = new StringRequestEntity(jsonString,
			                                            "application/json",
			                                            "UTF-8");
			mPut.setRequestEntity(requestEntity);
			int satus = client.executeMethod(mPut);
			output = mPut.getResponseBodyAsString( );
			
			mPut.releaseConnection( );
			System.out.println("satus : " + satus);
			System.out.println("output : " + output);
		}catch(Exception e){
			System.out.println("Exception in creating node in neo4j : " + e);
		}


}

	/**
	* generates json payload to be passed to relationship property web service
	* @param name
	* @param value
	* @return
	*/
	private String toJsonNameValuePairCollection(String name, String value) {
		return String.format("{ \"%s\" : \"%s\" }", name, value);
	}
	    
	}