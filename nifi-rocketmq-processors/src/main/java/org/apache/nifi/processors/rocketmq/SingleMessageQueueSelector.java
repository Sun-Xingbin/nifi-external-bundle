package org.apache.nifi.processors.rocketmq;

import org.apache.commons.lang3.math.NumberUtils;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;

import java.util.List;

public class SingleMessageQueueSelector implements MessageQueueSelector {

    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object index) {
        return mqs.get(NumberUtils.toInt(index.toString()));
    }

}
