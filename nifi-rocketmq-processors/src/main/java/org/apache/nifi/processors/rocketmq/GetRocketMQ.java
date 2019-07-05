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
import org.apache.rocketmq.client.consumer.store.ReadOffsetType;
import org.apache.rocketmq.client.impl.consumer.PullResultExt;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.*;
import java.util.concurrent.TimeUnit;

@CapabilityDescription("Fetches messages from RocketMQ")
@Tags({"RocketMQ", "Get"})
public class GetRocketMQ extends AbstractProcessor {

    private final List<PropertyDescriptor> DESCRIPTORS;
    private final Set<Relationship> RELATIONSHIPS;

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
        getLogger().info("start " + System.currentTimeMillis());
        getLogger().info("OnTrigger: " + String.valueOf(consumer));

        try {
            String topic = context.getProperty(TOPIC).getValue();
            Set<MessageQueue> mqs = consumer.fetchSubscribeMessageQueues(topic);

            getLogger().info("read topic:" + topic);

            for (MessageQueue messageQueue : mqs) {
                long offSet = consumer.getOffsetStore().readOffset(messageQueue, ReadOffsetType.MEMORY_FIRST_THEN_STORE);
                getLogger().info("read from offset:" + offSet);
                PullResultExt pullResult = (PullResultExt) consumer.pull(messageQueue, (String) null, offSet, 100);
                consumer.updateConsumeOffset(messageQueue, pullResult.getNextBeginOffset());
                getLogger().info("save offset:" + pullResult.getNextBeginOffset());
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
                                getLogger().info("Successfully received {} ({}) from RocketMQ in {} millis", new Object[]{flowFile, flowFile.getSize(), millis});
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
            }
        } catch (Exception e) {
            getLogger().error("consumer fail:", e);
        }
        getLogger().info("finished " + System.currentTimeMillis());
    }


    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        getLogger().info("OnScheduled:" + String.valueOf(consumer));
        if (consumer == null) {
            String consumerGroupId = context.getProperty(CONSUMER_GROUP_ID).getValue();
            String instanceName = context.getProperty(INSTANCE_NAME).getValue();
            consumer = new DefaultMQPullConsumer(consumerGroupId);
            consumer.setNamesrvAddr("192.168.4.64:9876");
            consumer.setInstanceName(instanceName);
            try {
                consumer.start();
            } catch (Exception e) {
                getLogger().error("Start fail", e.getStackTrace());
            }
        }
    }

    @OnDisabled
    public void onDisabled() {
        getLogger().info("OnDisabled:" + String.valueOf(consumer));
        invalidConsumer();
    }

    @OnUnscheduled
    public void onUnscheduled() {
        getLogger().info("OnUnscheduled:" + String.valueOf(consumer));
        invalidConsumer();
    }

    @OnStopped
    public void stopConsumer() {
        getLogger().info("OnStopped:" + String.valueOf(consumer));
        invalidConsumer();
    }

    private synchronized void invalidConsumer() {
        if (consumer != null) {
            consumer.shutdown();
        }
        consumer = null;
    }
}