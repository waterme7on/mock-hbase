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