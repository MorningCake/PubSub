package com.mc.pubsub.broker.controller;

import com.mc.pubsub.broker.api.Broker;
import lombok.RequiredArgsConstructor;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.stereotype.Controller;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Set;

@Controller
@MessageMapping("")
@RequiredArgsConstructor
public class BrokerController {
    private final Broker broker;

    @MessageMapping("initializeMicroservice")
    public Mono<String> initializeMicroservice(String serviceName, List<String> servicePublishingTopics, List<String> serviceSubscribingTopics) {
        broker.initializeMicroservice(serviceName, servicePublishingTopics, serviceSubscribingTopics);



        return Mono.just("Service init complete!");
    }

    @MessageMapping("push")
    public void pushMessage(String serviceName, String publishingTopic, String message) {
        Set<String> topics;
//        if (servicePublishedTopics.containsKey(serviceName)) {
//            topics = servicePublishedTopics.get(serviceName);
//        } else {
//            throw new IllegalArgumentException("Service " + serviceName + " is not registered in Broker!");
//        }
//        String topic;
//        if (topics.contains(publishingTopic)) {
//            topic = publishingTopic;
//        } else {
//            throw new IllegalArgumentException("Topic " + publishingTopic + " is not registered in Broker by service " + serviceName + "!");
//        }

    }
}
