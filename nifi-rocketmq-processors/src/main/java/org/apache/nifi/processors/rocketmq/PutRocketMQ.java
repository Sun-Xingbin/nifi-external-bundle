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
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.commons.io.IOUtils;
import org.apache.rocketmq.remoting.exception.RemotingException;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

@CapabilityDescription("Sends the contents of a FlowFile as a message to RocketMQ")
@Tags({"RocketMQ", "Put"})
public class PutRocketMQ extends AbstractProcessor {

    private final List<PropertyDescriptor> DESCRIPTORS;

    private final Set<Relationship> RELATIONSHIPS;

    private final Map<MessageQueue, Long> messageQueueOffsetMap = new LinkedHashMap<>();

    private final PropertyDescriptor TOPIC = new PropertyDescriptor.Builder()
            .name("TOPIC")
            .displayName("TOPIC")
            .description("Topic Name")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    private final PropertyDescriptor NAME_SRV_ADR = new PropertyDescriptor.Builder()
            .name("NAME_SRV_ADR")
            .displayName("NAME_SRV_ADR")
            .description("Name server Address Of the RocketMQ Cluster")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    private final PropertyDescriptor PRODUCER_GRP = new PropertyDescriptor.Builder()
            .name("PRODUCER_GRP")
            .displayName("PRODUCER_GRP")
            .description("producer group")
            .required(true)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .build();

    private final PropertyDescriptor QUEUE_NUM = new PropertyDescriptor.Builder()
            .name("QUEUE_NUM")
            .displayName("QUEUE_NUM")
            .description("queue num to be sent")
            .required(false)
            .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
            .build();
    private final Relationship SUCCESS = new Relationship.Builder()
            .name("SUCCESS")
            .description("Success relationship")
            .build();

    {
        List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(TOPIC);
        descriptors.add(NAME_SRV_ADR);
        descriptors.add(PRODUCER_GRP);
        descriptors.add(QUEUE_NUM);
        DESCRIPTORS = Collections.unmodifiableList(descriptors);
        RELATIONSHIPS = Collections.singleton(SUCCESS);
    }

    private volatile DefaultMQProducer producer;

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

        String topic = context.getProperty(TOPIC).getValue();

        try {
            List<FlowFile> flowFiles = session.get(10);

            for (final FlowFile flowFile : flowFiles) {
                session.read(flowFile, new InputStreamCallback() {
                    @Override
                    public void process(final InputStream in) throws IOException {
                        Message msg = new Message(topic, IOUtils.toByteArray(in));


                        try {
                            if(context.getProperty(QUEUE_NUM).getValue() == null) {
                                producer.send(msg);
                            }
                            else{
                                int queueNum = context.getProperty(QUEUE_NUM).asInteger();
                                producer.send(msg, new SingleMessageQueueSelector(), queueNum);
                            }
                        } catch (MQClientException e) {
                            getLogger().error(e.getErrorMessage());
                        } catch (RemotingException e) {
                            getLogger().error(e.getLocalizedMessage());
                        } catch (MQBrokerException e) {
                            getLogger().error(e.getErrorMessage());
                        } catch (InterruptedException e) {
                            getLogger().error(e.getLocalizedMessage());
                        }

                    }
                });
                session.transfer(flowFile, SUCCESS);
            }
        } catch (Exception e) {
            getLogger().error("produce fail:", e);
        }
    }


    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        try {
            if (producer == null) {
                String proGrp = context.getProperty(PRODUCER_GRP).getValue();
                String nameSrv = context.getProperty(NAME_SRV_ADR).getValue();
                producer = new DefaultMQProducer(proGrp);

                producer.setNamesrvAddr(nameSrv);
                producer.start();

            }
        } catch (Exception e) {
            getLogger().error("start fail", e.getStackTrace());
        }
    }

    @OnDisabled
    public void onDisabled() {
        invalidProducer();
    }

    @OnUnscheduled
    public void onUnscheduled() {
        getLogger().info("OnUnscheduled:" + String.valueOf(producer));
        invalidProducer();
    }

    @OnStopped
    public void stopProducer() {
        getLogger().info("OnStopped:" + String.valueOf(producer));
        invalidProducer();
    }

    private synchronized void invalidProducer() {
        if (producer != null) {
            producer.shutdown();
        }
        producer = null;
    }
}