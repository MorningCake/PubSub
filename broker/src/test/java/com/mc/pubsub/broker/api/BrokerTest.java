package com.mc.pubsub.broker.api;

import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class BrokerTest {

    @Test
    void getMicroserviceOrNull() {
    }

    @Test
    void getTopicOrNull() {
    }

    @Test
    void initializeMicroservice() {
        Microservice microservice = new Microservice("ms1");
        Microservice microservice2 = new Microservice("ms2");
        Broker broker = Broker.builder().microservices(Set.of(microservice, microservice2)).build();
        broker.initializeMicroservice("ms1", List.of("topic.one", "topic.two"), List.of());
        broker.initializeMicroservice("ms2", List.of("topic.three", "topic.four"), List.of("topic.one"));

        Topic topicOne = broker.getTopicOrNull("topic.one");
        UUID id1 = topicOne.pushMessage("HEllo world!");
        UUID id2 = topicOne.pushMessage("Goodbye world");
        assertEquals(topicOne.getPublisherQueue().size(), 2);
        assertEquals(topicOne.getSubscribersQueues().get(microservice2).size(), 2);
        Message message = topicOne.pullMessage(microservice2);
        assertEquals(message.getBody(), "HEllo world!");
        assertEquals(message.getId(), id1);
        assertEquals(topicOne.getPublisherQueue().size(), 2);
        assertEquals(topicOne.getSubscribersQueues().get(microservice2).size(), 1);
        Message message2 = topicOne.pullMessage(microservice2);
        assertEquals(message2.getBody(), "Goodbye world");
        assertEquals(message2.getId(), id2);
        assertEquals(topicOne.getPublisherQueue().size(), 2);
        assertEquals(topicOne.getSubscribersQueues().get(microservice2).size(), 0);
    }

    @Test
    void getTopics() {
    }

    @Test
    void getMicroservices() {
    }

    @Test
    void setTopics() {
    }

    @Test
    void setMicroservices() {
    }
}