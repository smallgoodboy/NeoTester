package com.neopack;

import java.util.Map;

public class CypherGenerater {

	
	/**
     * match a where (a)-[{re:'haha'}]->() match (a)-[r]->(b) return r
     */
    public String queryRelationStatement(Map<Object, Object>paraRelation){
    	String requestString = "";
    	if(paraRelation == null || paraRelation.size() == 0){
    		return null;
    	}
    	for(Object a:paraRelation.keySet()){
    		requestString += ("@"+ a + ":");
			Object aa = paraRelation.get(a);
			if(aa.getClass().toString().toLowerCase().contains("string")){
				requestString += "'" + aa + "'";
			}else if(aa.getClass().toString().toLowerCase().contains("integer")){
				requestString += aa;
			}
		}
    	if(requestString!=null && requestString.contains("@")){
    		requestString = requestString.substring(1);
    		requestString = requestString.replaceAll("@", " , ");
		}
    	requestString = "match a where (a)-[{" + requestString + "}]->() match (a)-[r]->(b) return a,r,b";
    	return requestString;
    }
    
    /**
     * match a where a.foo='bar15' match (a)-[r]->(b) return a,r,b
     */
    public String queryNodeStatement(Map<Object, Object>paraNode){
    	String requestString = "";
    	if(paraNode == null || paraNode.size() == 0){
    		return null;
    	}
    	for(Object a:paraNode.keySet()){
    		requestString += ("@a."+ a + "=");
			Object aa = paraNode.get(a);
			if(aa.getClass().toString().toLowerCase().contains("string")){
				requestString += "'" + aa + "'";
			}else if(aa.getClass().toString().toLowerCase().contains("integer")){
				requestString += aa;
			}
		}
    	if(requestString!=null && requestString.contains("@")){
    		requestString = requestString.substring(1);
    		requestString = requestString.replaceAll("@", " AND ");
		}
    	requestString = "match a where " + requestString + " match (a)-[r]->(b) return a,r,b";
    	return requestString;
    }
}
