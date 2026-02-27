package org.openphc.cce.collector.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.openphc.cce.collector.api.dto.CloudEventMessage;
import org.openphc.cce.collector.api.exception.KafkaPublishException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Publishes validated CloudEventMessages to the cce.events.inbound Kafka topic.
 * Partition key is the subject (patient UPID) to guarantee per-patient ordering.
 * Publishing is synchronous — blocks until Kafka acknowledges or fails.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class InboundEventProducer {

    private final KafkaTemplate<String, CloudEventMessage> kafkaTemplate;

    @Value("${cce.kafka.topics.inbound}")
    private String inboundTopic;

    /**
     * Synchronously publish a CloudEventMessage to Kafka.
     * Blocks until Kafka acknowledges the write or a timeout/error occurs.
     *
     * @param event the CloudEventMessage to publish
     * @return Kafka RecordMetadata on success
     * @throws KafkaPublishException if publishing fails
     */
    public RecordMetadata publish(CloudEventMessage event) {
        String key = event.getSubject(); // Partition by patient_id (UPID)

        try {
            SendResult<String, CloudEventMessage> result = kafkaTemplate.send(inboundTopic, key, event)
                    .get(30, TimeUnit.SECONDS);

            RecordMetadata metadata = result.getRecordMetadata();
            log.info("Published event {} to {}[{}]@{}",
                    event.getId(), metadata.topic(), metadata.partition(), metadata.offset());
            return metadata;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new KafkaPublishException(event, e);
        } catch (ExecutionException | TimeoutException e) {
            log.error("Failed to publish event {} to Kafka: {}", event.getId(), e.getMessage());
            throw new KafkaPublishException(event, e);
        }
    }
}
