package com.neopack;

import java.util.Vector;

import org.neo4j.shell.util.json.JSONArray;
import org.neo4j.shell.util.json.JSONException;
import org.neo4j.shell.util.json.JSONObject;

public class CypherReturnJsonResolver {

	
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
			if(tempJSONObject.has("start")){
				//It's a relation.!!
				//TODO:need to be took care of~
			}
			tempSelfPathSplit = tempSelfPath.split("/");
			tempList.add((Integer.valueOf(tempSelfPathSplit[tempSelfPathSplit.length-1])));
		}

		return tempList;
		//jsonObject.get(key);
	}
}
