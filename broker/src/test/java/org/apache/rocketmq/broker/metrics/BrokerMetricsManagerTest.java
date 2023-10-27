/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.broker.metrics;

import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import org.apache.rocketmq.common.attribute.TopicMessageType;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.remoting.protocol.header.SendMessageRequestHeader;
import org.junit.Test;

import static org.apache.rocketmq.broker.metrics.BrokerMetricsManager.getMessageType;
import static org.assertj.core.api.Assertions.assertThat;

public class BrokerMetricsManagerTest {

    @Test
    public void testNewAttributesBuilder() {
        Attributes attributes = BrokerMetricsManager.newAttributesBuilder().put("a", "b")
            .build();
        assertThat(attributes.get(AttributeKey.stringKey("a"))).isEqualTo("b");
    }

    @Test
    public void testCustomizedAttributesBuilder() {
        BrokerMetricsManager.attributesBuilderSupplier = () -> new AttributesBuilder() {
            private AttributesBuilder attributesBuilder = Attributes.builder();
            @Override
            public Attributes build() {
                return attributesBuilder.put("customized", "value").build();
            }

            @Override
            public <T> AttributesBuilder put(AttributeKey<Long> key, int value) {
                attributesBuilder.put(key, value);
                return this;
            }

            @Override
            public <T> AttributesBuilder put(AttributeKey<T> key, T value) {
                attributesBuilder.put(key, value);
                return this;
            }

            @Override
            public AttributesBuilder putAll(Attributes attributes) {
                attributesBuilder.putAll(attributes);
                return this;
            }
        };
        Attributes attributes = BrokerMetricsManager.newAttributesBuilder().put("a", "b")
            .build();
        assertThat(attributes.get(AttributeKey.stringKey("a"))).isEqualTo("b");
        assertThat(attributes.get(AttributeKey.stringKey("customized"))).isEqualTo("value");
    }

    @Test
    public void testGetMessageTypeNormal() {
        String topic = "topic";
        Message message = new Message(topic, "123".getBytes());
        SendMessageRequestHeader sendMessageRequestHeader = new SendMessageRequestHeader();

        sendMessageRequestHeader.setTopic(topic);
        sendMessageRequestHeader.setDefaultTopic("");
        sendMessageRequestHeader.setDefaultTopicQueueNums(0);
        sendMessageRequestHeader.setQueueId(0);
        sendMessageRequestHeader.setSysFlag(0);
        sendMessageRequestHeader.setBname("test");
        sendMessageRequestHeader.setProperties(MessageDecoder.messageProperties2String(message.getProperties()));

        assertThat(getMessageType(sendMessageRequestHeader)).isEqualTo(TopicMessageType.NORMAL);
    }

    @Test
    public void testGetMessageTypeTransaction() {
        String topic = "topic";
        Message message = new Message(topic, "123".getBytes());
        SendMessageRequestHeader sendMessageRequestHeader = new SendMessageRequestHeader();

        MessageAccessor.putProperty(message, MessageConst.PROPERTY_TRANSACTION_PREPARED, "true");
        sendMessageRequestHeader.setTopic(topic);
        sendMessageRequestHeader.setDefaultTopic("");
        sendMessageRequestHeader.setDefaultTopicQueueNums(0);
        sendMessageRequestHeader.setQueueId(0);
        sendMessageRequestHeader.setSysFlag(0);
        sendMessageRequestHeader.setBname("test");
        sendMessageRequestHeader.setProperties(MessageDecoder.messageProperties2String(message.getProperties()));

        assertThat(getMessageType(sendMessageRequestHeader)).isEqualTo(TopicMessageType.TRANSACTION);
    }
}