/**
 * Class for executing queries against an Oracle DB
 *
 * @author  Brad Mann brad.mann@marklogic.com
 */
package com.marklogic.rowbot;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import org.json.JSONObject;

class QueryBot implements Runnable {
	static final double NANO_TIME_DIV = 1000000000.0;
	private JSONObject queryObject; 
	private ExecutorService insertPool;
	private Connection connection;
	private RowBot rowBot;
	private boolean initFailed = false;
	
	public QueryBot(String connectionString, String user, String password, JSONObject queryObject, ExecutorService insertPool, RowBot rowBot) {
		this.queryObject = queryObject;
		this.insertPool = insertPool;
		this.rowBot = rowBot;
		
		if (queryObject.has("connectionString")) {
			connectionString = queryObject.getString("connectionString");
		}
		if (queryObject.has("username")) {
			user = queryObject.getString("username");
		}
		if (queryObject.has("password")) {
			password = queryObject.getString("password");
		}
		if (queryObject.has("database")) {
			connectionString = connectionString + "/" + queryObject.getString("database");
		}
		
		try {
			DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
			this.connection = DriverManager.getConnection(connectionString, user, password);
		} catch (SQLException e) {
			this.initFailed = true;
			this.rowBot.queryComplete(false, this.queryObject.getString("queryId"), this.queryObject.getString("query"), 0, 0, e.toString());
		}
	}
	
	public void run() {
		if (initFailed) {
			return;
		}
		
		double queryTime = 0;
		long queryStart = System.nanoTime();
		int rowNum = 0;
		try {
			String query = this.queryObject.getString("query");
			Statement statement = connection.createStatement();
			ResultSet rs = statement.executeQuery(query);
			ResultSetMetaData rsm = rs.getMetaData();
			int numColumns = rsm.getColumnCount();
			while (rs.next()) {
				rowNum++;
				Map<String, Object> rowMap = new HashMap<>();
				for (int i = 1; i <= numColumns; i++) {
					if (rsm.getColumnType(i) == Types.BLOB) {
						rowMap.put(rsm.getColumnName(i), rs.getBlob(i).getBinaryStream());
					} else {
						rowMap.put(rsm.getColumnName(i), rs.getString(i));
					}
				}
				Runnable insertThread = new InsertBot(rowMap, queryObject, rowNum, this.rowBot);
				insertPool.execute(insertThread);
			}
			
			long queryEnd = System.nanoTime();
			queryTime = (queryEnd - queryStart) / QueryBot.NANO_TIME_DIV;
			this.rowBot.queryComplete(true, this.queryObject.getString("queryId"), this.queryObject.getString("query"), rowNum, queryTime, null);
		} catch (SQLException e) {
			this.rowBot.queryComplete(false, this.queryObject.getString("queryId"), this.queryObject.getString("query"), rowNum, queryTime, e.toString());
		}
	} 
} 
