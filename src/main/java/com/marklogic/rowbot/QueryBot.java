/**
 * Class for executing queries against an Oracle DB
 *
 * @author  Brad Mann brad.mann@marklogic.com
 * @author  Steven Brockman steven.brockman@marklogic.com
 */
package com.marklogic.rowbot;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;
import java.util.Iterator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.json.JSONObject;

import com.marklogic.xcc.Session;

class QueryBot implements Runnable {
	static final double NANO_TIME_DIV = 1000000000.0;
	private JSONObject queryObject; 
	protected ExecutorService insertPool;
	private RowBot rowBot;
	private Session session;
	private Connection connection;
	private JSONObject connectionObj;
	protected static final int INSERT_QUEUE_SIZE = 1000000;
	protected LinkedBlockingQueue<Runnable> insertQueue;
	protected static final List<Runnable> activeTasks = Collections.synchronizedList(new ArrayList<Runnable>());

	public QueryBot(JSONObject connectionObj, JSONObject queryObject, RowBot rowBot) {
		this.queryObject = queryObject;
		this.rowBot = rowBot;
		this.connectionObj = connectionObj;
		this.insertQueue = new LinkedBlockingQueue<Runnable>(INSERT_QUEUE_SIZE);
		this.insertPool = new ThreadPoolExecutor(rowBot.numInsertThreads, rowBot.numInsertThreads, 0L, TimeUnit.MILLISECONDS, insertQueue);
	}

	protected void queryComplete(boolean success, String queryId, String query, int resultCount, double queryTime, String message) {
		String uri = rowBot.uriPrefix + rowBot.timestamp + "/" + queryId + "/rowbot-query-report.json";
		JSONObject results = new JSONObject();
		results.put("success", success);
		results.put("queryId", queryId);
		results.put("queryTime", queryTime);
		results.put("query", query);
		results.put("totalRows", resultCount);
		if (message != null) {
			results.put("message", message);
		}

		rowBot.insertJSONDoc(this.session, uri, results.toString(), null, new String[] {"rowbot-query-report"});

		synchronized(rowBot.queryReportUris) {
			rowBot.queryReportUris.add(uri);
		}
	}
	
	public void run() {
		activeTasks.add(this);
		String connectionString = connectionObj.getString("connectionString");

		this.session = rowBot.contentSource.newSession();

		try {
			Properties props = new Properties();
			JSONObject propsObj = connectionObj.getJSONObject("properties");
			Iterator<String> itr = propsObj.keys();
			while (itr.hasNext()) {
				String key = itr.next();
				props.put(key, propsObj.getString(key));
			}

			this.connection = DriverManager.getConnection(connectionString, props);
		} catch (SQLException e) {
			this.queryComplete(false, this.queryObject.getString("queryId"), this.queryObject.getString("query"), 0, 0, e.getMessage());
			return;
		}

		double queryTime = 0;
		long queryStart = System.nanoTime();
		int rowNum = 0;
		try {
			String query = this.queryObject.getString("query");
			try (
				Statement statement = connection.createStatement();
				ResultSet rs = statement.executeQuery(query);) 
			{
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
			}

			//signals pool to not accept more tasks.
			insertPool.shutdown();

			while (!insertPool.isTerminated()) {
				Thread.sleep(500);
			}

			long queryEnd = System.nanoTime();
			queryTime = (queryEnd - queryStart) / QueryBot.NANO_TIME_DIV;
			this.queryComplete(true, this.queryObject.getString("queryId"), this.queryObject.getString("query"), rowNum, queryTime, null);
		} catch (Exception e) {
			this.queryComplete(false, this.queryObject.getString("queryId"), this.queryObject.getString("query"), rowNum, queryTime, e.toString());
		} finally {
			// NOTE: connections are now closed in RowBot at the END.
			session.close();
			try {
				if (this.connection != null && this.connection.isValid(4)) {
					this.connection.close();
				}
			} catch (SQLException e) {
				// swallow
			}
			activeTasks.remove(this);
		}
	} 
}