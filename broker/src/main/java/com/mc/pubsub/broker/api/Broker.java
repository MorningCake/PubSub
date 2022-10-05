package com.mc.pubsub.broker.api;

import lombok.*;
import org.springframework.stereotype.Component;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Брокер. Отвечает за инициализацию микросервисов и при необходимости создает новые топики.
 * Хранит список микросервисов и все топики с данными.
 *
 * Выступает в качестве сервиса для контроллера (согласно DDD)
 */
@Component
@Getter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Broker {
    @Builder.Default
    private Set<Topic> topics = new HashSet<>();
    @Builder.Default
    private Set<Microservice> microservices = new HashSet<>();

    public Microservice getMicroserviceOrNull(String name) {
        return microservices.stream().filter(m -> m.getName().equals(name)).findFirst().orElse(null);
    }

    public Topic getTopicOrNull(String name) {
        return topics.stream().filter(t -> t.getTopicName().equals(name)).findFirst().orElse(null);
    }

    // todo ??? вопрос с транзакциями без БД ???
    public void initializeMicroservice(String serviceName, List<String> servicePublishingTopics,
                                       List<String> serviceSubscribingTopics) {
        // если сервиса еще нет - создать
        Microservice microservice = this.getMicroserviceOrNull(serviceName);
        if (microservice == null) {
            microservice = new Microservice(serviceName);

            // провалидировать список топиков сервиса на предмет пересечений

            // проверить дубликаты pub топиков в других сервисах

            // добавить новые pub топики

            // валидировать список подписок на предмет наличия топиков, которых нет в новом общем списке топиков

            // сохранить новые топики

            // добавить новые подписки в топики (новые очереди, заполненные всеми сообщениями)

            // добавить в микросервис списки топиков
        } else {
            // провалидировать список топиков сервиса на предмет пересечений
            microservice.checkPubAndSubTopicNames(new HashSet<>(servicePublishingTopics), new HashSet<>(serviceSubscribingTopics));

            // проверить дубликаты pub топиков в других сервисах
            Set<Topic> otherServicesPubTopicsDuplicates = topics.stream()
                    .filter(t -> servicePublishingTopics.contains(t.getTopicName()))
                    .filter(t -> !t.getPublisherMicroservice().getName().equals(serviceName))
                    .collect(Collectors.toSet());
            if (otherServicesPubTopicsDuplicates.size() > 0) {
                throw new IllegalArgumentException("Incorrect topic's Publishing (duplicate in other Microservices): " +
                        otherServicesPubTopicsDuplicates.stream().map(Topic::getTopicName).collect(Collectors.joining(", ")));
            }
            // выделить новые pub топики, если такие есть
            Set<String> allTopicNames = topics.stream().map(Topic::getTopicName).collect(Collectors.toSet());
            Microservice finalMicroservice = microservice;
            Set<Topic> newTopics = servicePublishingTopics.stream().filter(spt -> !allTopicNames.contains(spt))
                    .map(spt -> new Topic(spt, finalMicroservice)).collect(Collectors.toSet());

            // валидировать список подписок на предмет наличия топиков, которых нет в новом общем списке топиков
            Set<Topic> newAndCurrentTopics = new HashSet<>(topics);
            newAndCurrentTopics.addAll(newTopics);
            Set<String> newAndCurrentTopicsNames = newAndCurrentTopics.stream().map(Topic::getTopicName).collect(Collectors.toSet());
            Set<String> incorrectSubscribedTopicNames = serviceSubscribingTopics.stream()
                    .filter(sst -> !newAndCurrentTopicsNames.contains(sst))
                    .collect(Collectors.toSet());
            if (incorrectSubscribedTopicNames.size() > 0) {
                throw new IllegalArgumentException("Incorrect topics Subscribing (without Publishing): " +
                        String.join(", ", incorrectSubscribedTopicNames));
            }
            // todo как быть с транзакционностью вне БД - сага, распред транзакция, try catch )?
            // сохранить новые топики и добавить их в список публикаций
            topics.addAll(newTopics);
            microservice.addPublishingTopics(newTopics);

            // если есть новые подписки - в топиках добавить очереди (заполненные всеми сообщениями), в сервисе добавить топики
            Set<String> currentSubscribingTopicNames = microservice.getSubscribingTopics().stream()
                    .map(Topic::getTopicName)
                    .collect(Collectors.toSet());

            Set<Topic> newSubscribeTopics = serviceSubscribingTopics.stream()
                    .filter(sst -> !currentSubscribingTopicNames.contains(sst))
                    .map(this::getTopicOrNull)
                    .collect(Collectors.toSet());

            microservice.addSubscribingTopics(newSubscribeTopics);

            Microservice finalMicroservice1 = microservice;
            newSubscribeTopics.forEach(t -> t.addSubscriber(finalMicroservice1));

            // если есть удаленные подписки - в топиках удалить очереди, в сервисе удалить топики
            Set<Topic> deletedTopics = currentSubscribingTopicNames.stream()
                    .filter(cst -> !serviceSubscribingTopics.contains(cst))
                    .map(this::getTopicOrNull)
                    .collect(Collectors.toSet());

            microservice.deleteSubscribingTopics(deletedTopics);

            Microservice finalMicroservice2 = microservice;
            deletedTopics.forEach(t -> t.deleteSubscriber(finalMicroservice2));
        }

    }
}
