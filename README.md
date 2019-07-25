

# nifi-rocketmq-bundle

[RocketMQ](https://rocketmq.apache.org/) processor for [Apache NIFI](https://nifi.apache.org).

GetRocketMQ processor, which reads message from RocketMQ.

# nifi-redis-bundle

PutRedis processor, which extractd key and value from flow file, then put them into redis.



## How to use it?

`mvn clean install`

Copy `nifi-rocketmq-nar/target/nifi-rocketmq-nar-0.1.0.nar` to `$NIFI_HOME/lib/` and restart NIFI.
Copy `nifi-redis-nar/target/nifi-redis-nar-0.1.0.nar` to `$NIFI_HOME/lib/` and restart NIFI.


## License

Apache 2.0
