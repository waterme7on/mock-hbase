# mock-hbase

*mock hbase for thread model paper*

## Architecture

- RegionServer
    - HLog
    - MemStore
    - HFile
    - RPC Server
- HMaster
    - RegionServerManager
        - RegionConfiguration
        - RegionServerScheduler
- ZookeeperHandler
- HDFSHandler
- YCSBApi

## TODO

...

## Build

simply run:
```
mvn package -DskipTests && ./bin/hbase.sh
```


menu:
- Resolve dependencies: `mvn dependency:resolve`
- Build codes to executable JAR file: `mvn package`
- Run tests: `mvn test`
- Run main processes: `java -jar hbase-server/target/hbase-server-1.0.jar`
- Run with debug: `java -Dorg.slf4j.simpleLogger.defaultLogLevel=debug -jar hbase-server/target/hbase-server-1.0.jar`

## Contribution

1. Code
2. Open a pull request
3. Merge