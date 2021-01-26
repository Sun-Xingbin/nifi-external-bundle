package org.apache.nifi.processors.redis;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.annotation.lifecycle.OnUnscheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StringUtils;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.*;

@CapabilityDescription("Write String to Redis")
@Tags({"PutRedis", "Redis"})
public class PutRedis extends AbstractProcessor {

    private volatile JedisCluster jedisCluster;

    private final ObjectMapper mapper = new ObjectMapper();

    private static final Set<Relationship> RELATIONSHIPS;

    private static final List<PropertyDescriptor> DESCRIPTORS;

    private static final PropertyDescriptor KEY = new PropertyDescriptor.Builder()
            .name("KEY")
            .displayName("KEY")
            .description("KEY")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    private static final PropertyDescriptor VALUE = new PropertyDescriptor.Builder()
            .name("VALUE")
            .displayName("VALUE")
            .description("VALUE")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    private static final PropertyDescriptor TTL_SECOND = new PropertyDescriptor.Builder()
            .name("TTL_SECOND")
            .displayName("TTL_SECOND")
            .description("TTL_SECOND")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    private static final Relationship SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Files that have been successfully written to redis are transferred to this relationship")
            .build();

    private static final Relationship FAILURE = new Relationship.Builder()
            .name("failure")
            .description(
                    "Files that could not be written to redis for some reason are transferred to this relationship")
            .build();

    static {
        final Set<Relationship> relationshipSet = new HashSet<>();
        relationshipSet.add(SUCCESS);
        relationshipSet.add(FAILURE);
        RELATIONSHIPS = Collections.unmodifiableSet(relationshipSet);

        List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(KEY);
        descriptors.add(VALUE);
        descriptors.add(TTL_SECOND);
        DESCRIPTORS = Collections.unmodifiableList(descriptors);
    }

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onTrigger(final ProcessContext context,
                          final ProcessSession session) throws ProcessException {
        ensureJedisPrepared();

        getLogger().info("OnTrigger: " + jedisCluster);

        FlowFile flowfile = session.get();

        session.read(flowfile, in -> {
            String key = context.getProperty(KEY).getValue();
            String topic = context.getProperty(VALUE).getValue();
            int ttlSecond = Integer.parseInt(context.getProperty(TTL_SECOND).getValue());
            String json = IOUtils.toString(in);
            Map<String, String> map = mapper.readValue(json, new TypeReference<Map<String, String>>() {
            });
            String redisKey = map.get(key);
            String redisValue = map.get(topic);
            if (StringUtils.isBlank(redisKey) || StringUtils.isBlank(redisValue)) {
                return;
            }
            jedisCluster.set(redisKey, redisValue);
            jedisCluster.expire(redisKey, ttlSecond);
        });
        session.getProvenanceReporter().send(flowfile, "redis");
        session.transfer(flowfile, SUCCESS);
    }


    private synchronized void ensureJedisPrepared() {
        if (jedisCluster != null) {
            return;
        }

        synchronized (PutRedis.class) {
            if (jedisCluster != null) {
                return;
            }
            try {
                Set<HostAndPort> jedisClusterNodes = new HashSet<>();
                // TODO set host and port by yourself
                HostAndPort jedisNode = new HostAndPort("ip", 7003);
                jedisClusterNodes.add(jedisNode);

                GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
                poolConfig.setMaxTotal(2000);
                poolConfig.setMaxIdle(100);
                poolConfig.setMaxWaitMillis(1000 * 100);
                poolConfig.setTestOnBorrow(true);

                jedisCluster = new JedisCluster(jedisClusterNodes, poolConfig);
                getLogger().info("jedis prepared :" + jedisCluster);
            } catch (Exception e) {
                getLogger().error("jedis prepare fail: ", e);
                throw new RuntimeException(e);
            }
        }
    }

    @OnDisabled
    public void onDisabled() {
        getLogger().info("OnDisabled:" + jedisCluster);
        invalidConsumer();
    }

    @OnUnscheduled
    public void onUnscheduled() {
        getLogger().info("OnUnscheduled:" + jedisCluster);
        invalidConsumer();
    }

    @OnStopped
    public void stopConsumer() {
        getLogger().info("OnStopped:" + jedisCluster);
        invalidConsumer();
    }

    private synchronized void invalidConsumer() {
        if (jedisCluster != null) {
            try {
                jedisCluster.close();
                jedisCluster = null;
            } catch (Exception e) {
                getLogger().error("jedisCluster.close fail ", e);
            }
        }
    }

}