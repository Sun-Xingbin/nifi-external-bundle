package org.apache.nifi.processors.rocketmq;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
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
import org.apache.rocketmq.client.consumer.DefaultMQPullConsumer;
import org.apache.rocketmq.client.consumer.PullStatus;
import org.apache.rocketmq.client.impl.consumer.PullResultExt;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

@CapabilityDescription("Fetches messages from RocketMQ")
@Tags({"RocketMQ", "Get"})
public class GetRocketMQ extends AbstractProcessor {

    private final List<PropertyDescriptor> descriptors;

    private final Set<Relationship> relationships;

    private final ReentrantLock lock = new ReentrantLock();

    private final Map<MessageQueue, Long> messageQueueOffsetMap = new LinkedHashMap<>();

    private final PropertyDescriptor topicDescriptor = new PropertyDescriptor.Builder()
            .name("TOPIC")
            .displayName("TOPIC")
            .description("TOPIC")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    private final PropertyDescriptor consumerGroupIdDescriptor = new PropertyDescriptor.Builder()
            .name("CONSUMER_GROUP_ID")
            .displayName("CONSUMER_GROUP_ID")
            .description("CONSUMER_GROUP_ID")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    private final PropertyDescriptor instanceNameDescriptor = new PropertyDescriptor.Builder()
            .name("INSTANCE_NAME")
            .displayName("INSTANCE_NAME")
            .description("INSTANCE_NAME")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    private final Relationship successRelationship = new Relationship.Builder()
            .name("SUCCESS")
            .description("Success relationship")
            .build();

    {
        List<PropertyDescriptor> currentDescriptors = new ArrayList<>();
        currentDescriptors.add(topicDescriptor);
        currentDescriptors.add(consumerGroupIdDescriptor);
        currentDescriptors.add(instanceNameDescriptor);
        descriptors = Collections.unmodifiableList(currentDescriptors);
        relationships = Collections.singleton(successRelationship);
    }

    private volatile DefaultMQPullConsumer consumer;

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public void onTrigger(ProcessContext context, final ProcessSession session) throws ProcessException {
        if (!lock.tryLock()) {
            getLogger().error("get lock fail");
            return;
        }

        try {
            getLogger().info("OnTrigger: " + consumer);

            String topic = context.getProperty(this.topicDescriptor).getValue();
            Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues(topic);
            for (MessageQueue messageQueue : mqs) {
                long offSet = messageQueueOffsetMap.get(messageQueue);
                PullResultExt pullResult = (PullResultExt) consumer.pull(messageQueue, (String) null, offSet, 100);
                if (pullResult.getPullStatus() == PullStatus.FOUND) {
                    List<MessageExt> messageExtList = pullResult.getMsgFoundList();
                    for (MessageExt messageExt : messageExtList) {
                        long start = System.nanoTime();
                        FlowFile flowFile = session.create();
                        flowFile = session.write(flowFile, out -> out.write(messageExt.getBody()));
                        if (flowFile.getSize() == 0L) {
                            session.remove(flowFile);
                        } else {
                            final long millis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);
                            session.getProvenanceReporter().receive(flowFile, "rockemq://", "Received RocketMQ RocketMQMessage", millis);
                            session.transfer(flowFile, successRelationship);
                        }
                    }
                } else {
                    getLogger().error("not found message queue: " + messageQueue);
                }

                consumer.updateConsumeOffset(messageQueue, pullResult.getNextBeginOffset());
                messageQueueOffsetMap.put(messageQueue, pullResult.getNextBeginOffset());

                getLogger().info("topic: " + topic
                        + ", message queue: " + messageQueue
                        + ", start offset: " + offSet
                        + ", next offset: " + pullResult.getNextBeginOffset());
            }
        } catch (Exception e) {
            getLogger().error("consumer fail:", e);
        } finally {
            lock.unlock();
            getLogger().info("release lock");
        }
    }


    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        getLogger().info("OnScheduled:" + consumer);
        if (consumer != null) {
            return;
        }

        synchronized (GetRocketMQ.class) {
            if (consumer != null) {
                return;
            }
            try {
                String consumerGroupId = context.getProperty(this.consumerGroupIdDescriptor).getValue();
                String instanceName = context.getProperty(this.instanceNameDescriptor).getValue();
                consumer = new DefaultMQPullConsumer(consumerGroupId);
                // TODO set the address by yourself
                consumer.setNamesrvAddr("ip:port");
                consumer.setInstanceName(instanceName);
                consumer.start();

                messageQueueOffsetMap.clear();
                String topic = context.getProperty(this.topicDescriptor).getValue();
                Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues(topic);
                for (MessageQueue messageQueue : mqs) {
                    long offSet = consumer.maxOffset(messageQueue);
                    messageQueueOffsetMap.put(messageQueue, offSet);
                }
            } catch (Exception e) {
                getLogger().error("start fail", e);
            }
        }
    }

    @OnDisabled
    public void onDisabled() {
        getLogger().info("OnDisabled:" + consumer);
        invalidConsumer();
    }

    @OnUnscheduled
    public void onUnscheduled() {
        getLogger().info("OnUnscheduled:" + consumer);
        invalidConsumer();
    }

    @OnStopped
    public void stopConsumer() {
        getLogger().info("OnStopped:" + consumer);
        invalidConsumer();
    }

    private synchronized void invalidConsumer() {
        if (consumer != null) {
            consumer.shutdown();
        }
        consumer = null;
    }
}