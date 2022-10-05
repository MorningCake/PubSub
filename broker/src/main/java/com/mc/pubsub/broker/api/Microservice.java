package com.mc.pubsub.broker.api;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Сервис. Регистрируется брокером.
 * Имеет список Топиков, куда публикует сообщения, и список топиков, на которые подписан.
 * Не должен слушать топики, куда сам пушит.
 */
@Getter
@EqualsAndHashCode(onlyExplicitlyIncluded = true)
@ToString(onlyExplicitlyIncluded = true)
public class Microservice {

    @EqualsAndHashCode.Include
    @ToString.Include
    private final String name;

    private Set<Topic> publishingTopics;
    private Set<Topic> subscribingTopics;

    public Microservice(String name) {
        this.name = name;
        this.publishingTopics = new HashSet<>();
        this.subscribingTopics = new HashSet<>();
    }

    public Microservice(String name, Set<Topic> publishingTopics, Set<Topic> subscribingTopics) {
        this.name = name;
        checkPubAndSubTopics(publishingTopics, subscribingTopics);
        this.publishingTopics = publishingTopics;
        this.subscribingTopics = subscribingTopics;
    }

    public void addPublishingTopics(Collection<Topic> topics) {
        publishingTopics.addAll(topics);
    }

    public void addSubscribingTopics(Collection<Topic> topics) {
        subscribingTopics.addAll(topics);
    }

    public void deleteSubscribingTopics(Collection<Topic> topics) {
        if (!subscribingTopics.containsAll(topics)) {
            throw new IllegalArgumentException("One or more topics hasn't found to delete!");
        } else {
            subscribingTopics.removeAll(topics);
        }
    }

    /**
     * Проверка того, что нет пересечений в списках топиков
     * @param publishingTopics топики для публикаций
     * @param subscribingTopics топики, на которые подписан
     */
    public void checkPubAndSubTopics(Set<Topic> publishingTopics, Set<Topic> subscribingTopics) {
        Set<Topic> incorrectTopics = new HashSet<>(publishingTopics);
        incorrectTopics.retainAll(subscribingTopics);
        if (!incorrectTopics.isEmpty()) {
            throw new IllegalArgumentException("Incorrect topics (both Published and Subscribed): " +
                    incorrectTopics.stream().map(Topic::getTopicName).collect(Collectors.joining(", "))
            );
        }
    }

    public void checkPubAndSubTopicNames(Set<String> publishingTopics, Set<String> subscribingTopics) {
        Set<String> incorrectTopics = new HashSet<>(publishingTopics);
        incorrectTopics.retainAll(subscribingTopics);
        if (!incorrectTopics.isEmpty()) {
            throw new IllegalArgumentException("Incorrect topics (both Published and Subscribed): " +
                    String.join(", ", incorrectTopics)
            );
        }
    }
}
