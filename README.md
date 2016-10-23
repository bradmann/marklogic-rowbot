## Description

RowBot is a java application that turns Oracle database rows into MarkLogic documents. It connects directly to Oracle and MarkLogic DBs, issues queries against the Oracle DB, and then inserts the results of the queries as single-level documents in MarkLogic.

## Dependencies

You will need to obtain 3 jar files:
- marklogic-xcc-8.0.5.jar
- ojdbc6.jar
- org.json.jar

## Building

Place the 3 jar files above in the jars/ directory and run "gradle" from the root directory

## Running

From within the dist directory, run ./rowbot.sh <rowbot-job-file>.json

## Reference

Look at dist/sample-rowbot-job.json for detailed syntax of RowBot job configurations.