/**
 * Application for executing queries against an Oracle DB and inserting the
 * results into MarkLogic via XCC
 *
 * @author  Brad Mann brad.mann@marklogic.com
 */
package com.marklogic.rowbot;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.marklogic.xcc.Content;
import com.marklogic.xcc.ContentCreateOptions;
import com.marklogic.xcc.ContentFactory;
import com.marklogic.xcc.ContentSource;
import com.marklogic.xcc.ContentSourceFactory;
import com.marklogic.xcc.Session;

public class RowBot implements Runnable {
	static final double NANO_TIME_DIV = 1000000000.0;
	static final int QUERY_QUEUE_SIZE = 100000;
	static final int INSERT_QUEUE_SIZE = 5000000;
	static final int INSERT_REPORT_BATCH_SIZE = 10000;
	static final int STATUS_UPDATE_INTERVAL = 20;
	static final int MAX_INSERT_RETRIES = 5;
	static final int INSERT_RETRY_SLEEP_MILLIS = 1000;
	//private Session session;
	protected ContentSource contentSource;
	String timestamp;
	String jobName;
	String uriPrefix;
	String type;
	private List<String> queryReportUris;
	private List<String> insertReportUris;
	private ConcurrentHashMap<String, List<JSONObject>> insertResults;
	private JSONArray queries;
	private JSONObject mlConfig;
	private ExecutorService queryPool;
	private ExecutorService insertPool;
	private LinkedBlockingQueue<Runnable> queryQueue;
	private LinkedBlockingQueue<Runnable> insertQueue;
	private HashMap<String, JSONObject> connHM;
	private String rowbotReportUri;
	private HashMap<String, Connection> queryConnHM = new HashMap<String, Connection>();
		
	private HashMap<String, JSONObject> parseConnections(JSONArray connArray) {
		HashMap<String, JSONObject> hm = new HashMap<String, JSONObject>();
		
		Iterator<?> itr = connArray.iterator();
		while (itr.hasNext()) {
			JSONObject connectionObj = (JSONObject) itr.next();
			String key = connectionObj.getString("key");
			hm.put(key, connectionObj);
		}

		return hm;
	}
	
	public RowBot(String configString) {
		System.out.println("Hi. I'm RowBot.");
		
		JSONObject config = new JSONObject(configString);
		this.queries = config.getJSONArray("queries");
		int numQueryThreads = config.getInt("queryThreads");
		int numInsertThreads = config.getInt("insertThreads");
		this.mlConfig = config.getJSONObject("marklogic");
		this.jobName = config.has("jobName") ? config.getString("jobName") : null;
		this.uriPrefix = config.has("uriPrefix") ? config.getString("uriPrefix") : "/rowbot/";
		this.type = config.has("type") ? config.getString("type") : null;
		
		System.out.println("Parsed RowBot job. Found " + String.valueOf(this.queries.length()) + " SQL queries to execute.");

		TimeZone tz = TimeZone.getTimeZone("UTC");
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		df.setTimeZone(tz);
		this.timestamp = df.format(new Date());
		
		this.queryQueue = new LinkedBlockingQueue<Runnable>(QUERY_QUEUE_SIZE);
		this.insertQueue = new LinkedBlockingQueue<Runnable>(INSERT_QUEUE_SIZE);
		
		this.queryPool = new ThreadPoolExecutor(numQueryThreads, numQueryThreads, 0L, TimeUnit.MILLISECONDS, queryQueue);
		this.insertPool = new ThreadPoolExecutor(numInsertThreads, numInsertThreads, 0L, TimeUnit.MILLISECONDS, insertQueue);
		
		contentSource = ContentSourceFactory.newContentSource(mlConfig.getString("hostname"), Integer.parseInt(mlConfig.getString("port")), mlConfig.getString("username"), mlConfig.getString("password"));
		//this.session = contentSource.newSession();
		this.insertResults = new ConcurrentHashMap<String, List<JSONObject>>();
		this.queryReportUris = Collections.synchronizedList(new ArrayList<String>());
		this.insertReportUris = Collections.synchronizedList(new ArrayList<String>());
		this.rowbotReportUri = this.uriPrefix + this.timestamp + "/rowbot-report.json";

		try {
			// Register jdbc driver
			DriverManager.registerDriver(new oracle.jdbc.driver.OracleDriver());
			this.connHM = parseConnections(config.getJSONArray("connections"));

		} catch (JSONException e) {
			System.out.println("Error parsing JSON in connections array." + e.getMessage());

		} catch (SQLException e) {
			Session session = contentSource.newSession();
			JSONObject json = new JSONObject();
			json.put("timestamp", timestamp);
			json.put("status", "error");
			json.put("errorMessage", e.getMessage());
			json.put("type", this.type);
			session = contentSource.newSession();
			insertJSONDoc(session, this.rowbotReportUri, json.toString(), null, new String[] {"rowbot-report"});
			session.close();
			System.out.println("Error registering driver:" + e.getMessage());
		}
	}
	
	protected void queryComplete(Session session, boolean success, String queryId, String query, int resultCount, double queryTime, String message) {
		String uri = this.uriPrefix + this.timestamp + "/" + queryId + "/rowbot-query-report.json";
		JSONObject results = new JSONObject();
		results.put("success", success);
		results.put("queryId", queryId);
		results.put("queryTime", queryTime);
		results.put("query", query);
		results.put("totalRows", resultCount);
		if (message != null) {
			results.put("message", message);
		}

		insertJSONDoc(session, uri, results.toString(), null, new String[] {"rowbot-query-report"});

		synchronized(queryReportUris) {
			queryReportUris.add(uri);
		}
	}
	
	protected void insertComplete(Session session, boolean success, String queryId, String uri, String message) {
		JSONObject resultObject = new JSONObject();
		resultObject.put("success", success);
		resultObject.put("uri", uri);
		if (message != null) {
			resultObject.put("message", message);
		}

		synchronized(insertResults) {
			insertResults.putIfAbsent(queryId, Collections.synchronizedList(new ArrayList<JSONObject>()));
			List<JSONObject> resultList = insertResults.get(queryId);
			synchronized(resultList) {
				resultList.add(resultObject);
				if (resultList.size() >= RowBot.INSERT_REPORT_BATCH_SIZE) {
					this.writeInsertionReport(session, queryId);
				}
			}
		}
	}
	
	protected void writeInsertionReport(Session session, String queryId) {
		JSONObject json = new JSONObject();
		String insertId = UUID.randomUUID().toString();
		List<JSONObject> batchResults = this.insertResults.get(queryId);
		if (batchResults.isEmpty()) {
			return;
		}
		json.put("timestamp", this.timestamp);
		json.put("queryId", queryId);
		json.put("insertId", insertId);
		for (JSONObject insertResult : batchResults) {
			json.append("results", insertResult);
		}
		String uri = this.uriPrefix + this.timestamp + "/" + queryId + "/" + insertId + "/rowbot-insert-report.json";
		this.insertJSONDoc(session, uri, json.toString(), null, new String[] {"rowbot-insert-report"});
		synchronized(insertReportUris) {
			this.insertReportUris.add(uri);
		}
		this.insertResults.put(queryId, Collections.synchronizedList(new ArrayList<JSONObject>()));
	}
	
	protected JSONObject insertJSONDoc(Session session, String uri, String json, JSONObject permissions, String[] collections) {
		ContentCreateOptions options = ContentCreateOptions.newJsonInstance();
		options.setCollections(collections);
		Content content = ContentFactory.newContent(uri, json, options);
		Exception exception = null;
		for (int i = 0; i < MAX_INSERT_RETRIES; i++) {
			try {
				session.insertContent(content);
				return new JSONObject("{\"success\": true, \"uri\": \"" + uri + "\"}");
			} catch (Exception e) {
				try {
					Thread.sleep(INSERT_RETRY_SLEEP_MILLIS);
				} catch (InterruptedException e1) {
					
				}
				exception = e;
			}
		}
		JSONObject result = new JSONObject("{\"success\": false, \"uri\": \"" + uri + "\"}");
		result.put("message", exception.toString());
		return result;
	}
	
	protected JSONObject insertBinaryDoc(Session session, String uri, InputStream binary, JSONObject permissions, String[] collections) {
		ContentCreateOptions options = ContentCreateOptions.newBinaryInstance();
		options.setCollections(collections);
		Exception exception = null;
		for (int i = 0; i < MAX_INSERT_RETRIES; i++) {
			try {
				Content content = ContentFactory.newContent(uri, binary, options);
				session.insertContent(content);
				return new JSONObject("{\"success\": true, \"uri\": \"" + uri + "\"}");
			} catch (Exception e) {
				try {
					Thread.sleep(INSERT_RETRY_SLEEP_MILLIS);
				} catch (InterruptedException e1) {
					
				}
				exception = e;
			}
		}
		JSONObject result = new JSONObject("{\"success\": false, \"uri\": \"" + uri + "\"}");
		result.put("message", exception.toString());
		return result;
	}
	
	private static Content createJSONContent(String uri, String[] collections, String content) {
		ContentCreateOptions options = ContentCreateOptions.newJsonInstance();
		options.setCollections(collections);
		return ContentFactory.newContent(uri, content, options);
	}
	
	public static void main(String[] args) {
		HashMap<String, String> argsMap = new HashMap<String, String>();
		for (String argument : args) {
			if (argument.startsWith("--")) {
				argsMap.put(argument.split("=")[0].substring(2), argument.split("=")[1]);
			} else {
				if (argsMap.containsKey("__arguments")) {
					argsMap.put("__arguments", argsMap.get("__arguments") + "," + argument);
				} else {
					argsMap.put("__arguments", argument);
				}
			}
		}

		try {
			String configString = new String(Files.readAllBytes(FileSystems.getDefault().getPath(argsMap.get("__arguments"))));
			RowBot rowBot = new RowBot(configString);
			rowBot.runRowBotJob();
		} catch (IOException e1) {
			System.out.println("Unable to read RowBot config document. Exiting.");
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}
	
	public void runRowBotJob() throws InterruptedException, JSONException {
		long startTime = System.nanoTime();
		
		// Insert an initial RowBot report document with a status of "started"
		
		Session session = contentSource.newSession();
		JSONObject json = new JSONObject();
		json.put("timestamp", timestamp);
		json.put("status", "started");
		json.put("type", this.type);
		insertJSONDoc(session, rowbotReportUri, json.toString(), null, new String[] {"rowbot-report"});
		session.close();
		
		try {
			Thread rowBotThread = new Thread(this);
			rowBotThread.start();
			
			JSONObject status = new JSONObject();
			JSONObject queryStatus = new JSONObject();
			JSONObject insertStatus = new JSONObject();
			
			int statusCounter = STATUS_UPDATE_INTERVAL;
			while (rowBotThread.isAlive() || !queryPool.isTerminated() || !insertPool.isTerminated()) {
				if (!rowBotThread.isAlive() && !queryPool.isShutdown()) {
					queryPool.shutdown();
				}
				
				if (!rowBotThread.isAlive() && queryPool.isTerminated() && !insertPool.isShutdown()) {
					insertPool.shutdown();
				}
				
				if (statusCounter % STATUS_UPDATE_INTERVAL == 0) {
					queryStatus.put("running", ((ThreadPoolExecutor) queryPool).getActiveCount());
					queryStatus.put("queued", (QUERY_QUEUE_SIZE - queryQueue.remainingCapacity()));
					insertStatus.put("running", ((ThreadPoolExecutor) insertPool).getActiveCount());
					insertStatus.put("queued", (INSERT_QUEUE_SIZE - insertQueue.remainingCapacity()));
					status.put("queries", queryStatus);
					status.put("inserts", insertStatus);
					System.out.println(status.toString());
				}
				statusCounter = (statusCounter == 1) ? STATUS_UPDATE_INTERVAL : statusCounter - 1;
				
				if (rowBotThread.isAlive() || !queryPool.isTerminated() || !insertPool.isTerminated()) {
					Thread.sleep(250);
				}
			}
			
			System.out.println("Job complete. Writing remaining insertion reports.");
			
			session = contentSource.newSession();
			// Gather up all the insertion results and write documents out for them.
			for (String queryId : insertResults.keySet()) {
				writeInsertionReport(session, queryId);
			}
			
			long endTime = System.nanoTime();
			
			// Insert the final RowBot report document with a status of "complete"
			json = new JSONObject();
			if (jobName != null) { json.put("jobName", jobName); }
			json.put("timestamp", timestamp);
			json.put("totalRuntime", (endTime - startTime) / NANO_TIME_DIV);
			json.put("insertReportUris", insertReportUris);
			json.put("queryReportUris", queryReportUris);
			if (type != null) { json.put("type", type); }
			json.put("status", "complete");
			insertJSONDoc(session, rowbotReportUri, json.toString(), null, new String[] {"rowbot-report"});

			System.out.println("RowBot is complete. Bye.");

		} finally {
			session.close();
			for (Connection curConn : this.queryConnHM.values()) {
				try {
					curConn.close();
				} catch (SQLException e) {
					// swallow
				}
			}
		}
	}

	@Override
	public void run() {
		for (int i = 0; i < this.queries.length(); i++) {
			JSONObject queryObject = this.queries.getJSONObject(i);
			String queryId = (queryObject.has("queryId")) ? queryObject.getString("queryId")  : UUID.randomUUID().toString();
			queryObject.put("queryId", queryId);
			queryObject.put("timestamp", this.timestamp);
			String dbKey = queryObject.getString("database");
			Runnable queryThread;

			queryThread = new QueryBot((JSONObject) this.connHM.get(dbKey), queryObject, insertPool, this);
			queryPool.execute(queryThread);
		}
	}
	
	public synchronized void addConnection(String key, Connection conn) {
		this.queryConnHM.put(key, conn);
	}
}
