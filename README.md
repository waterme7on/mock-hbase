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

- Resolve dependencies: `mvn dependency:resolve`
- Build codes to executable JAR file: `mvn package`
- Run tests: `mvn test`

## Contribution

1. Code
2. Open a pull request
3. Merge