

# nifi-rocketmq-bundle

[RocketMQ](https://rocketmq.apache.org/) processor for [Apache NIFI](https://nifi.apache.org).

GetRocketMQ processor, which reads message from RocketMQ.

# nifi-redis-bundle

PutRedis processor, which extracts key and value from flow file, then puts them into redis.



## How to use it?

1. find all "TODO" tags and set the configurations

2. `mvn clean install`

3. Copy `nifi-rocketmq-nar/target/nifi-rocketmq-nar-0.1.0.nar` to `$NIFI_HOME/lib/`

4. Copy `nifi-redis-nar/target/nifi-redis-nar-0.1.0.nar` to `$NIFI_HOME/lib/`

5. restart NIFI


## License

Apache 2.0
