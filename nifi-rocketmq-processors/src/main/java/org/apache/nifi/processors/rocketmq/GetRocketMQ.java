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
import org.apache.rocketmq.client.impl.consumer.PullResultExt;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

@CapabilityDescription("Fetches messages from RocketMQ")
@Tags({"RocketMQ", "Get"})
public class GetRocketMQ extends AbstractProcessor {

    private final List<PropertyDescriptor> DESCRIPTORS;

    private final Set<Relationship> RELATIONSHIPS;

    private final ReentrantLock lock = new ReentrantLock();

    private final Map<MessageQueue, Long> messageQueueOffsetMap = new LinkedHashMap<>();

    private final PropertyDescriptor TOPIC = new PropertyDescriptor.Builder()
            .name("TOPIC")
            .displayName("TOPIC")
            .description("TOPIC")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    private final PropertyDescriptor CONSUMER_GROUP_ID = new PropertyDescriptor.Builder()
            .name("CONSUMER_GROUP_ID")
            .displayName("CONSUMER_GROUP_ID")
            .description("CONSUMER_GROUP_ID")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    private final PropertyDescriptor INSTANCE_NAME = new PropertyDescriptor.Builder()
            .name("INSTANCE_NAME")
            .displayName("INSTANCE_NAME")
            .description("INSTANCE_NAME")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    private final Relationship SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Success relationship")
            .build();

    {
        List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(TOPIC);
        descriptors.add(CONSUMER_GROUP_ID);
        descriptors.add(INSTANCE_NAME);
        DESCRIPTORS = Collections.unmodifiableList(descriptors);
        RELATIONSHIPS = Collections.singleton(SUCCESS);
    }

    private volatile DefaultMQPullConsumer consumer;

    @Override
    public List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return DESCRIPTORS;
    }

    @Override
    public Set<Relationship> getRelationships() {
        return RELATIONSHIPS;
    }

    @Override
    public void onTrigger(ProcessContext context, final ProcessSession session) throws ProcessException {
        if (!lock.tryLock()) {
            getLogger().error("get lock fail");
            return;
        }

        try {
            getLogger().error("OnTrigger: " + String.valueOf(consumer));

            String topic = context.getProperty(TOPIC).getValue();
            Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues(topic);
            for (MessageQueue messageQueue : mqs) {
                long offSet = messageQueueOffsetMap.get(messageQueue);
                PullResultExt pullResult = (PullResultExt) consumer.pull(messageQueue, (String) null, offSet, 100);
                switch (pullResult.getPullStatus()) {
                    case FOUND:
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
                                session.transfer(flowFile, SUCCESS);
                            }
                        }
                        break;
                    case NO_MATCHED_MSG:
                        break;
                    case NO_NEW_MSG:
                        break;
                    case OFFSET_ILLEGAL:
                        break;
                    default:
                        break;
                }
                consumer.updateConsumeOffset(messageQueue, pullResult.getNextBeginOffset());
                messageQueueOffsetMap.put(messageQueue, pullResult.getNextBeginOffset());
                getLogger().error("topic: " + topic
                        + ", message queue: " + messageQueue
                        + ", start offset: " + offSet
                        + ", next offset: " + pullResult.getNextBeginOffset());
            }
        } catch (Exception e) {
            getLogger().error("consumer fail:", e);
        } finally {
            lock.unlock();
            getLogger().error("release lock");
        }
        getLogger().error("finished " + System.currentTimeMillis());
    }


    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        try {
            getLogger().error("OnScheduled:" + String.valueOf(consumer));
            if (consumer == null) {
                String consumerGroupId = context.getProperty(CONSUMER_GROUP_ID).getValue();
                String instanceName = context.getProperty(INSTANCE_NAME).getValue();
                consumer = new DefaultMQPullConsumer(consumerGroupId);
                consumer.setNamesrvAddr("192.168.4.64:9876");
                consumer.setInstanceName(instanceName);
                consumer.start();

                messageQueueOffsetMap.clear();
                String topic = context.getProperty(TOPIC).getValue();
                Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues(topic);
                for (MessageQueue messageQueue : mqs) {
                    long offSet = consumer.maxOffset(messageQueue);
                    messageQueueOffsetMap.put(messageQueue, offSet);
                }
            }
        } catch (Exception e) {
            getLogger().error("start fail", e.getStackTrace());
        }
    }

    @OnDisabled
    public void onDisabled() {
        getLogger().error("OnDisabled:" + String.valueOf(consumer));
        invalidConsumer();
    }

    @OnUnscheduled
    public void onUnscheduled() {
        getLogger().error("OnUnscheduled:" + String.valueOf(consumer));
        invalidConsumer();
    }

    @OnStopped
    public void stopConsumer() {
        getLogger().error("OnStopped:" + String.valueOf(consumer));
        invalidConsumer();
    }

    private synchronized void invalidConsumer() {
        if (consumer != null) {
            consumer.shutdown();
        }
        consumer = null;
    }
}