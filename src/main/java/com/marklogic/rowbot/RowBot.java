/**
 * Application for executing queries against an Oracle DB and inserting the
 * results into MarkLogic via XCC
 *
 * @author  Brad Mann brad.mann@marklogic.com
 */

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
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
import com.marklogic.xcc.exceptions.RequestException;

public class RowBot implements Runnable {
	static final double NANO_TIME_DIV = 1000000000.0;
	static final int QUERY_QUEUE_SIZE = 100000;
	static final int INSERT_QUEUE_SIZE = 5000000;
	private Session session;
	private String timestamp;
	private List<String> queryReportUris;
	private ConcurrentHashMap<String, List<JSONObject>> insertResults;
	private JSONArray queries;
	private JSONObject mlConfig;
	private JSONObject oConfig;
	private ExecutorService queryPool;
	private ExecutorService insertPool;
	private LinkedBlockingQueue<Runnable> queryQueue;
	private LinkedBlockingQueue<Runnable> insertQueue;
	
	public RowBot(String configString) {
		System.out.println("Hi. I'm RowBot.");
		
		JSONObject config = new JSONObject(configString);
		this.queries = config.getJSONArray("queries");
		int numQueryThreads = config.getInt("queryThreads");
		int numInsertThreads = config.getInt("insertThreads");
		this.mlConfig = config.getJSONObject("marklogic");
		this.oConfig = config.getJSONObject("oracle");
		
		System.out.println("Parsed RowBot job. Found " + String.valueOf(this.queries.length()) + " SQL queries to execute.");
		
		TimeZone tz = TimeZone.getTimeZone("UTC");
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
		df.setTimeZone(tz);
		this.timestamp = df.format(new Date());
		
		this.queryQueue = new LinkedBlockingQueue<Runnable>(QUERY_QUEUE_SIZE);
		this.insertQueue = new LinkedBlockingQueue<Runnable>(INSERT_QUEUE_SIZE);
		
		this.queryPool = new ThreadPoolExecutor(numQueryThreads, numQueryThreads, 0L, TimeUnit.MILLISECONDS, queryQueue);
		this.insertPool = new ThreadPoolExecutor(numInsertThreads, numInsertThreads, 0L, TimeUnit.MILLISECONDS, insertQueue);
		
		ContentSource contentSource = ContentSourceFactory.newContentSource(mlConfig.getString("hostname"), Integer.parseInt(mlConfig.getString("port")), mlConfig.getString("username"), mlConfig.getString("password"));
		this.session = contentSource.newSession();
		this.insertResults = new ConcurrentHashMap<String, List<JSONObject>>();
		this.queryReportUris = Collections.synchronizedList(new ArrayList<String>());
	}
	
	protected void queryComplete(boolean success, String queryId, String query, int resultCount, double queryTime, String message) {
		String uri = "/rowbot/" + this.timestamp + "/" + queryId + "/rowbot-query-report.json";
		JSONObject results = new JSONObject();
		results.put("success", success);
		results.put("queryId", queryId);
		results.put("queryTime", queryTime);
		results.put("query", query);
		results.put("totalRows", resultCount);
		if (message != null) {
			results.put("message", message);
		}
		
		try {
			this.session.insertContent(
					RowBot.createJSONContent(uri, new String[] {"rowbot-query-report"}, results.toString()));
			this.queryReportUris.add(uri);
		} catch (RequestException e) {
			e.printStackTrace();
		}
	}
	
	protected void insertComplete(boolean success, String queryId, String uri, String message) {
		JSONObject resultObject = new JSONObject();
		resultObject.put("success", success);
		resultObject.put("uri", uri);
		if (message != null) {
			resultObject.put("message", message);
		}

		insertResults.putIfAbsent(queryId, Collections.synchronizedList(new ArrayList<JSONObject>()));
		insertResults.get(queryId).add(resultObject);
	}
	
	protected JSONObject insertJSONDoc(String uri, String json, JSONObject permissions, String[] collections) {
		ContentCreateOptions options = ContentCreateOptions.newJsonInstance();
		options.setCollections(collections);
		Content content = ContentFactory.newContent(uri, json, options);
		try {
			session.insertContent(content);
			return new JSONObject("{\"success\": true, \"uri\": \"" + uri + "\"}");
		} catch (RequestException e) {
			JSONObject result = new JSONObject("{\"success\": false, \"uri\": \"" + uri + "\"}");
			result.put("message", e.toString());
			return result;
		}
	}
	
	protected JSONObject insertBinaryDoc(String uri, InputStream binary, JSONObject permissions, String[] collections) {
		ContentCreateOptions options = ContentCreateOptions.newBinaryInstance();
		options.setCollections(collections);
		try {
			Content content = ContentFactory.newContent(uri, binary, options);
			session.insertContent(content);
			return new JSONObject("{\"success\": true, \"uri\": \"" + uri + "\"}");
		} catch (RequestException | IOException e) {
			JSONObject result = new JSONObject("{\"success\": false, \"uri\": \"" + uri + "\"}");
			result.put("message", e.toString());
			return result;
		}
	}
	
	private static Content createJSONContent(String uri, String[] collections, String content) {
		ContentCreateOptions options = ContentCreateOptions.newJsonInstance();
		options.setCollections(collections);
		return ContentFactory.newContent(uri, content, options);
	}
	
	public static void main(String[] args) {
		long startTime = System.nanoTime();
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
			
			Thread rowBotThread = new Thread(rowBot);
			rowBotThread.start();
			
			JSONObject status = new JSONObject();
			JSONObject queryStatus = new JSONObject();
			JSONObject insertStatus = new JSONObject();
			
			while (rowBotThread.isAlive() || !rowBot.queryPool.isTerminated() || !rowBot.insertPool.isTerminated()) {
				Thread.sleep(5000);
				queryStatus.put("running", ((ThreadPoolExecutor) rowBot.queryPool).getActiveCount());
				queryStatus.put("queued", (QUERY_QUEUE_SIZE - rowBot.queryQueue.remainingCapacity()));
				insertStatus.put("running", ((ThreadPoolExecutor) rowBot.insertPool).getActiveCount());
				insertStatus.put("queued", (INSERT_QUEUE_SIZE - rowBot.insertQueue.remainingCapacity()));
				status.put("queries", queryStatus);
				status.put("inserts", insertStatus);
				System.out.println(status.toString());
				
				if (!rowBotThread.isAlive() && !rowBot.queryPool.isTerminated()) {
					rowBot.queryPool.shutdown();
				}
				
				if (!rowBotThread.isAlive() && rowBot.queryPool.isTerminated()) {
					rowBot.insertPool.shutdown();
				}
			}
			
			System.out.println("Job complete. Writing insertion reports.");
			
			ArrayList<String> insertReportUris = new ArrayList<String>();
			// Gather up all the insertion results and write documents out for them.
			for (String queryId : rowBot.insertResults.keySet()) {
				JSONObject json = new JSONObject();
				json.put("timestamp", rowBot.timestamp);
				json.put("queryId", queryId);
				for (JSONObject insertResult : rowBot.insertResults.get(queryId)) {
					json.append("results", insertResult);
				}
				String uri = "/rowbot/" + rowBot.timestamp + "/" + queryId +  "/rowbot-insert-report.json";
				rowBot.insertJSONDoc(uri, json.toString(), null, new String[] {"rowbot-insert-report"});
				insertReportUris.add(uri);
			}
			
			long endTime = System.nanoTime();
			
			JSONObject json = new JSONObject();
			json.put("timestamp", rowBot.timestamp);
			json.put("totalRuntime", (endTime - startTime) / NANO_TIME_DIV);
			json.put("insertReportUris", insertReportUris);
			json.put("queryReportUris", rowBot.queryReportUris);
			String uri = "/rowbot/" + rowBot.timestamp + "/rowbot-report.json";
			rowBot.insertJSONDoc(uri, json.toString(), null, new String[] {"rowbot-report"});
			System.out.println("RowBot is complete. Bye.");
		} catch (IOException e1) {
			System.out.println("Unable to read RowBot config document. Exiting.");
			System.exit(1);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (JSONException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void run() {
		for (int i = 0; i < this.queries.length(); i++) {
			JSONObject queryObject = this.queries.getJSONObject(i);
			String queryId = (queryObject.has("queryId")) ? queryObject.getString("queryId")  : UUID.randomUUID().toString();
			queryObject.put("queryId", queryId);
			queryObject.put("timestamp", this.timestamp);
			Runnable queryThread;
			String connectionString = this.oConfig.getString("connectionString");
			queryThread = new QueryBot(connectionString, oConfig.getString("username"), oConfig.getString("password"), queryObject, insertPool, this);
			queryPool.execute(queryThread);   
		}
	}  
}
