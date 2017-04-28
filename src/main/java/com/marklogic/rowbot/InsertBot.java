/**
 * Class for inserting documents into MarkLogic via XCC
 *
 * @author  Brad Mann brad.mann@marklogic.com
 */
package com.marklogic.rowbot;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import org.json.JSONObject;

import com.marklogic.xcc.Session;

class InsertBot implements Runnable {  
	private Map<String, Object> row;
	private JSONObject queryObject;
	private int rowNum;
	private RowBot rowBot;
	private Session session;
	
	public InsertBot(Map<String, Object> row, JSONObject queryObject, int rowNum, RowBot rowBot){  
		this.row = row;
		this.queryObject = queryObject;
		this.rowNum = rowNum;
		this.rowBot = rowBot;
		this.session = rowBot.contentSource.newSession();
	}
	
	public void run() {
		HashMap<String, String> stringMap = new HashMap<>();
		HashMap<String, InputStream> binaryMap = new HashMap<>();
		for (String key : this.row.keySet()) {
			Object value = this.row.get(key);
			if (value instanceof String) {
				stringMap.put(key, (String) value);
			} else if (value instanceof InputStream) {
				String binaryUUID = UUID.randomUUID().toString();
				binaryMap.put(binaryUUID, (InputStream) value);
				stringMap.put(key, "BINARY:" + binaryUUID);
			}
		}
		
		JSONObject json = new JSONObject(stringMap);
		String[] collections = this.queryObject.getJSONArray("collections").toList().toArray(new String[0]);
		String uriBase = rowBot.uriPrefix + this.queryObject.getString("timestamp") + "/" + this.queryObject.getString("queryId") + "/";
		
		for (String key : binaryMap.keySet()) {
			String uri = uriBase + "binary/" + key;
			JSONObject result = rowBot.insertBinaryDoc(session, uri, binaryMap.get(key), null, collections);
			String message = (result.has("message")) ? result.getString("message") : null;
			rowBot.insertComplete(session, result.getBoolean("success"), this.queryObject.getString("queryId"), uri, message);
		}
		
		String uri = uriBase + String.valueOf(this.rowNum) + ".json";
		
		JSONObject result = rowBot.insertJSONDoc(session, uri, json.toString(), null, collections);
		String message = (result.has("message")) ? result.getString("message") : null;
		rowBot.insertComplete(session, result.getBoolean("success"), this.queryObject.getString("queryId"), uri, message);
		session.close();
	}
} 