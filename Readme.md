# Mini HBase client for Impala row estimates
This mini-client is the standalone version of the one implemented in 2.10 Impala FE to estimate row counts

## Building

```
mvn package

```

## Usage

```
java -jar HBaseTest-1.0-SNAPSHOT.jar tablename startkey endkey
```