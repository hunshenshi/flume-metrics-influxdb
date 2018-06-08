# flume-metrics-influxdb
Flume metrics sink for influxdb

tested on Flume version 1.8.0

## Start Flume agent

* copy the jar to the classpath and start flume agent with the following content:

```
bin/flume-ng agent -n a1 -c conf -f conf/flume.conf -Dflume.monitoring.type=influxdb -Dflume.monitoring.url=http://127.0.0.1:8086 -Dflume.monitoring.database=testDB -Dflume.monitoring.username=hadoop -Dflume.monitoring.password=hadoop -Dflume.monitoring.cluster=flume2
```
