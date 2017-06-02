## Description
RowBot is a java application that turns Oracle database rows into MarkLogic documents. It connects directly to Oracle and MarkLogic DBs, issues queries against the Oracle DB, and then inserts the results of the queries as single-level documents in MarkLogic.

RowBot uses a threadpool for both queries and inserts, so it easily be scaled for very large operations.

## Dependencies
You will need to obtain 3 jar files:
- marklogic-xcc-8.0.5.jar
- ojdbc6.jar
- org.json.jar

## Building
Place the 3 jar files above in the jars/ directory and run "gradle" from the root directory

## Running
From within the dist directory, run
```
./rowbot.sh <rowbot-job-file>.json
```

## Rowbot Processing Steps
1. RowBot starts by constructing a [rowbot-report](#sample-rowbot-report) document within the target MarkLogic database (via the defined XCC AppServer.)  The uri format for this file will be something like:
```
/rowbot/2017-05-31T15:12:55.972Z/rowbot-report.json
```

2. RowBot then spawns N QueryBot Threads (as defined by "queryThreads" in the [rowbot-job-file](#reference-rowbot-job-file).)  Each thread is given a query to work on from the list of queries in the "queries" section.  

	* N InsertBot threads are created per QueryBot thread (as defined by "insertThreads" in the [rowbot-job-file](#reference-rowbot-job-file).
		* Each insert thread will update a corresponding InsertReport that lists all document uris inserted as a [rowbot-insert-report](#sample-rowbot-insert-report)
	* When all row insertions into MarkLogic are complete, QueryBot creates a [rowbot-query-report](#sample-rowbot-query-report) document.  The uri format for this file will be similar to:

```
/rowbot/2017-05-31T15:12:55.972Z/d4d9fea9-4484-401d-9e79-f70354a7b9e2/rowbot-query-report.json
```

Note: Each Query thread manages it's own insert threads

3. When all QueryBot Threads are complete, RowBot updates the [rowbot-report](#sample-rowbot-report) file with updated insertion information.

4. RowBot exits

### Sample rowbot-report

```
{
"jobName": "foo", 
"totalRuntime": 6.554132867, 
"insertReportUris": [
"/rowbot/2017-05-31T15:12:55.972Z/23155fc9-eb2c-486a-a041-f6592e43ccc4/43e31760-e18a-4491-94a2-a8c009655b75/rowbot-insert-report.json", 
"/rowbot/2017-05-31T15:12:55.972Z/2e79055a-01af-4de2-8e65-02e1a406369a/1a540dd6-fd9f-44fe-bfb1-7734ebb595f1/rowbot-insert-report.json", 
"/rowbot/2017-05-31T15:12:55.972Z/8c8600c2-67e7-4044-a71a-62b039ea6823/0cb9af13-7e1a-47d9-b328-606f2554faa9/rowbot-insert-report.json", 
"/rowbot/2017-05-31T15:12:55.972Z/163cb8fd-b327-4ed4-8dd0-8f8ab760ed1b/efe60112-762f-4cf7-8fb4-cad2593be30e/rowbot-insert-report.json", 
"/rowbot/2017-05-31T15:12:55.972Z/f2d0b1bb-f12c-41c1-8039-1106f578bade/fc10f9fe-74f1-43cd-8fd0-b9cd2876143f/rowbot-insert-report.json", 
"/rowbot/2017-05-31T15:12:55.972Z/775d3706-4fed-437f-91aa-69765be47675/a5d8d099-e9c0-4fd9-ae38-157dcf4fc339/rowbot-insert-report.json", 
"/rowbot/2017-05-31T15:12:55.972Z/81704f01-71a1-4c5f-912a-a90f1c90dd06/4252c47a-7019-4298-87b0-2a61bd266fe8/rowbot-insert-report.json", 
"/rowbot/2017-05-31T15:12:55.972Z/66ae30f7-9f6a-462b-bd44-1ad1699d7d48/50750275-a880-47b0-b016-6ea85d568bb5/rowbot-insert-report.json", 
"/rowbot/2017-05-31T15:12:55.972Z/6d1bc54c-8bef-49a6-aef4-5066b4684b2d/1fb856cc-cc18-4a3a-a6da-db8480988607/rowbot-insert-report.json", 
"/rowbot/2017-05-31T15:12:55.972Z/4945ee93-9963-4357-9dca-7d20b8b155a9/1fc83228-3be0-49fd-b62c-1f67d28e9092/rowbot-insert-report.json", 
"/rowbot/2017-05-31T15:12:55.972Z/e8631bc5-35e5-4d16-b73d-8c2c0aa9badf/6ccc1181-9b06-4914-ba6a-cad28ceedf7e/rowbot-insert-report.json", 
"/rowbot/2017-05-31T15:12:55.972Z/ae8bbddc-5f7f-456b-a9a2-1cd2d0af87ab/71e50d3a-9d85-4c57-8758-ba23c932871e/rowbot-insert-report.json", 
"/rowbot/2017-05-31T15:12:55.972Z/da2bead2-3ba4-4673-bf78-0ef8e412796f/715982a9-0a49-42f2-b90b-7d0c0b35f5fd/rowbot-insert-report.json", 
"/rowbot/2017-05-31T15:12:55.972Z/1f1184aa-60fd-4d88-9ecf-cd9479dc1da4/4d815209-ca9b-48b1-abcf-850322cbad7e/rowbot-insert-report.json", 
"/rowbot/2017-05-31T15:12:55.972Z/168fb610-77dc-4d44-9d2e-f04ccf517643/729762b5-67fd-47ab-8445-0827f6dd68fa/rowbot-insert-report.json", 
"/rowbot/2017-05-31T15:12:55.972Z/66cd4490-a230-4a0c-8b8f-da338b1ad5dd/b516fd7d-3c6a-44a3-8b43-8927ad68e472/rowbot-insert-report.json"
]
, 
"queryReportUris": [
"/rowbot/2017-05-31T15:12:55.972Z/66ae30f7-9f6a-462b-bd44-1ad1699d7d48/rowbot-query-report.json", 
"/rowbot/2017-05-31T15:12:55.972Z/f2d0b1bb-f12c-41c1-8039-1106f578bade/rowbot-query-report.json", 
"/rowbot/2017-05-31T15:12:55.972Z/6d1bc54c-8bef-49a6-aef4-5066b4684b2d/rowbot-query-report.json", 
"/rowbot/2017-05-31T15:12:55.972Z/2e79055a-01af-4de2-8e65-02e1a406369a/rowbot-query-report.json", 
"/rowbot/2017-05-31T15:12:55.972Z/da2bead2-3ba4-4673-bf78-0ef8e412796f/rowbot-query-report.json", 
"/rowbot/2017-05-31T15:12:55.972Z/23155fc9-eb2c-486a-a041-f6592e43ccc4/rowbot-query-report.json", 
"/rowbot/2017-05-31T15:12:55.972Z/81704f01-71a1-4c5f-912a-a90f1c90dd06/rowbot-query-report.json", 
"/rowbot/2017-05-31T15:12:55.972Z/775d3706-4fed-437f-91aa-69765be47675/rowbot-query-report.json", 
"/rowbot/2017-05-31T15:12:55.972Z/66cd4490-a230-4a0c-8b8f-da338b1ad5dd/rowbot-query-report.json", 
"/rowbot/2017-05-31T15:12:55.972Z/1f1184aa-60fd-4d88-9ecf-cd9479dc1da4/rowbot-query-report.json", 
"/rowbot/2017-05-31T15:12:55.972Z/4945ee93-9963-4357-9dca-7d20b8b155a9/rowbot-query-report.json", 
"/rowbot/2017-05-31T15:12:55.972Z/8c8600c2-67e7-4044-a71a-62b039ea6823/rowbot-query-report.json", 
"/rowbot/2017-05-31T15:12:55.972Z/163cb8fd-b327-4ed4-8dd0-8f8ab760ed1b/rowbot-query-report.json", 
"/rowbot/2017-05-31T15:12:55.972Z/ae8bbddc-5f7f-456b-a9a2-1cd2d0af87ab/rowbot-query-report.json", 
"/rowbot/2017-05-31T15:12:55.972Z/e8631bc5-35e5-4d16-b73d-8c2c0aa9badf/rowbot-query-report.json", 
"/rowbot/2017-05-31T15:12:55.972Z/168fb610-77dc-4d44-9d2e-f04ccf517643/rowbot-query-report.json"
]
, 
"timestamp": "2017-05-31T15:12:55.972Z", 
"status": "complete"
}
```
### Sample rowbot-insert-report
```
{
"results": [
{
"success": true, 
"uri": "/rowbot/2017-05-31T15:12:55.972Z/2e79055a-01af-4de2-8e65-02e1a406369a/1.json"
}
, 
{
"success": true, 
"uri": "/rowbot/2017-05-31T15:12:55.972Z/2e79055a-01af-4de2-8e65-02e1a406369a/2.json"
}
,
* * *
{
"success": true, 
"uri": "/rowbot/2017-05-31T15:12:55.972Z/2e79055a-01af-4de2-8e65-02e1a406369a/1000.json"
}
]
, 
"timestamp": "2017-05-31T15:12:55.972Z", 
"queryId": "2e79055a-01af-4de2-8e65-02e1a406369a", 
"insertId": "1a540dd6-fd9f-44fe-bfb1-7734ebb595f1"
}
```

### Sample rowbot-query-report
```
{
"success": true, 
"query": "SELECT * FROM FOO", 
"queryTime": 2.64581898, 
"totalRows": 1000, 
"queryId": "2e79055a-01af-4de2-8e65-02e1a406369a"
}
```

## Reference rowbot-job-file
RowBot's JSON job format is documented below.
```
{
	// Specify the queries to run in the queries section. Supports an aribtrary number of queries.
	"queries": [
		{
			"database": "XE", // The SID of a database to connect to
			"query": "SELECT * FROM FOO", // The query to run
			"collections": ["query1-staging"] // The collections to apply to each result document
		}
	],
	"queryThreads": 5, // The number of query threads
	"insertThreads": 1, // The number of insertion threads per query (minimum must be 1)
	"docType": "json", // Currently, only JSON is supported
	"marklogic": { // MarkLogic connection information
		"hostname": "localhost",
		"port": "8041",
		"authentication": "digest",
		"username": "admin",
		"password": "admin"
	},
	"connections": [
		{	"key": "XE",
			"connectionString": "jdbc:oracle:thin:@//localhost:1521",
			"properties": {
				"user": "system",
				"password": "password",
				"defaultRowPrefetch": "20"  // optionally set additional connection options
			}
		}
	],
	"jobName": "foo" // Sample name used to track job reports
}
```