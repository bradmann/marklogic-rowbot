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

## Reference
RowBot's JSON job format is documented below.
```
{
    // Specify the queries to run in the queries section. Supports an aribtrary number of queries.
	"queries": [
		{
			"database": "XE", // The SID of a database to connect to
			"query": "SELECT * FROM FOO", // The query to run
			"collections": ["query1-staging"] // The collections to apply to each result document
		},
		{
			"connectionString": "jdbc:oracle:thin:@//someotherhost:1521", // Override the default connection string
			"username": "admin2", // Override the default username
			"password": "password", // Override the default password
			"query": "SELECT * FROM BAR",
			"collections": ["query2-staging"]
		}
	],
	"queryThreads": 10, // The number of query threads
	"insertThreads": 10, // The number of insertion threads
	"docType": "json", // Currently, only JSON is supported
	"marklogic": { // MarkLogic connection information
		"hostname": "localhost",
		"port": "8041",
		"authentication": "digest",
		"username": "admin",
		"password": "admin"
	},
	"oracle": { // Oracle connection information
		"connectionString": "jdbc:oracle:thin:@//localhost:1521",
		"username": "system",
		"password": "password"
	}
}
```
