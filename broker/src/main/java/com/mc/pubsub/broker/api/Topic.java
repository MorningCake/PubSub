package com.mc.pubsub.broker.api;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.time.OffsetDateTime;
import java.util.*;

/**
 * Топик. Создается при инициализции сервиса, если в списке публикуемых топиков указан новый.
 * Содержит очередь, в которую пушит Издатель (но из которой сообщения не вытягиваются), и список очередей для Подписчиков.
 * В очереди для Подписчиков также пушатся сообщения, и они служат для вытягивания сообщения Подписчиками.
 *
 * При инициализации сервисов в топике может быть изменен список подписчиков.
 * Каждый новый подписчик должен получить полную очередь сообщений
 */
@Getter
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@ToString(onlyExplicitlyIncluded = true)
public class Topic {

    @EqualsAndHashCode.Include
    @ToString.Include
    private final String topicName;

    private final Microservice publisherMicroservice;
    private final Deque<Message> publisherQueue;

    private Map<Microservice, Deque<Message>> subscribersQueues;


    public Topic(String topicName, Microservice publisherMicroservice) {
        this.topicName = topicName;
        this.publisherQueue = new ArrayDeque<>();
        this.publisherMicroservice = publisherMicroservice;
        subscribersQueues = new HashMap<>();
//        subscribersQueues.put(publisherMicroservice, new ArrayDeque<>());
    }

    /**
     * Запушить сообщение в основную очередь, а также в очереди подписчиков
     * @param msgBody тело сообщения
     * @return ID сообщения
     */
    public UUID pushMessage(String msgBody) {
        UUID id = UUID.randomUUID();
        Message message = new Message(id, OffsetDateTime.now(), msgBody);
        publisherQueue.addLast(message);
        subscribersQueues.values().forEach(q -> q.addLast(message));
        return id;
    }

    /**
     * Получить первое в очереди сообщение для подписчика (с выталкиванием из очереди подписчика)
     * @param subscriber подписчик
     * @return первое в очереди сообщение
     */
    public Message pullMessage(Microservice subscriber) {
        if (!subscribersQueues.containsKey(subscriber)) {
            throw new IllegalArgumentException("Wrong Subscriber Service name: " + subscriber);
        }
        Deque<Message> queue = subscribersQueues.get(subscriber);
        return queue.pollFirst();
    }

    public void addSubscriber(Microservice microservice) {
        subscribersQueues.put(microservice, new ArrayDeque<>(publisherQueue));
    }

    public void addSubscribers(Collection<Microservice> microservices) {
        microservices.forEach(this::addSubscriber);
    }

    public void deleteSubscriber(Microservice microservice) {
        if (!subscribersQueues.containsKey(microservice)) {
            throw new IllegalArgumentException("Subscriber " + microservice + "not found!");
        } else {
            subscribersQueues.remove(microservice);
        }
    }

    public void deleteSubscribers(Collection<Microservice> microservices) {
        microservices.forEach(this::deleteSubscriber);
    }


//    private void checkTopicName(String topicName) {
//        if (!topicName.equals(this.topicName)) {
//            throw new IllegalArgumentException("Wrong topic name!");
//        }
//    }
}
