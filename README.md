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
clear && mvn install -DskipTests && ./bin/hbase-daemon.sh start master
```

run standalone zookeeper:
```
# zookeeper server
docker pull zookeeper
docker run --name hbase-zookeeper --restart always -d zookeeper
# add zookeeper's ip to conf/hbase-site.xml
# create 

# zookeeper client (for test)
docker run -it --rm --link hbase-zookeeper:zookeeper zookeeper zkCli.sh -server zookeeper
```


menu:
- Resolve dependencies: `mvn dependency:resolve`
- Build codes to executable JAR file: `mvn package`
- Run tests: `mvn test`
- Run main processes: `java -jar hbase-server/target/hbase-server-1.0.jar`
- Run with debug: `java -Dorg.slf4j.simpleLogger.defaultLogLevel=debug -jar hbase-server/target/hbase-server-1.0.jar`
- Unit tests: ``

## Dev Environment

```
Java:
openjdk version "1.8.0_352"
OpenJDK Runtime Environment (build 1.8.0_352-8u352-ga-1~20.04-b08)
OpenJDK 64-Bit Server VM (build 25.352-b08, mixed mode)

OS:
Distributor ID: Ubuntu
Description:    Ubuntu 20.04.5 LTS
Release:        20.04
Codename:       focal

Maven:
Apache Maven 3.6.3
Maven home: /usr/share/maven
Java version: 1.8.0_352, vendor: Private Build, runtime: /usr/lib/jvm/java-8-openjdk-arm64/jre
Default locale: en_US, platform encoding: ANSI_X3.4-1968
OS name: "linux", version: "5.10.104-linuxkit", arch: "aarch64", family: "unix"

```

## Contribution

1. Code
2. Open a pull request
3. Merge