package ru.gx.sender.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import ru.gx.core.channels.SerializeMode;
import ru.gx.core.utils.StringUtils;
import ru.gx.sender.dto.SendMessageResult;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

@Slf4j
@Service
public class KafkaService {
    @NotNull
    private final String defaultKafkaServer;

    public KafkaService(
            @NotNull @Value("${service.kafka.server}") final String defaultKafkaServer
    ) {
        super();
        this.defaultKafkaServer = defaultKafkaServer;
    }

    // <Server, <Topic, Producer>>
    private final Map<String, Map<String, Producer<Long, ?>>> producers = new HashMap<>();

    private Properties producerProperties(
            @NotNull final String kafkaServer,
            @NotNull final SerializeMode serializeMode
    ) {
        final var props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class);
        if (serializeMode == SerializeMode.JsonString) {
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        } else {
            props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class);
        }
        return props;
    }

    private Producer<Long, ?> prepareProducer(
            @NotNull final String kafkaServer,
            @NotNull final String topic,
            @NotNull final SerializeMode serializeMode
    ) {
        final var serverTopics = this.producers
                .computeIfAbsent(kafkaServer, k -> new HashMap<>());

        return serverTopics
                .computeIfAbsent(topic, k -> new KafkaProducer<>(
                        producerProperties(kafkaServer, serializeMode)
                ));
    }

    @SuppressWarnings("unchecked")
    @NotNull
    public SendMessageResult sendSerializedMessage(
            @Nullable final String kafkaServerParam,
            @NotNull final String topic,
            final int partition,
            @NotNull final SerializeMode serializeMode,
            @NotNull final Object serializedMessage
    ) throws ExecutionException, InterruptedException {
        log.info(
                "START sendMessage(): " +
                        "kafkaServerParam = " + StringUtils.isNullObject(kafkaServerParam, "null") + ", " +
                        "topic = " + topic + ", " +
                        "partition = " + partition + ", " +
                        "serializeMode = " + serializeMode + ", " +
                        "serializedMessage: \n" + serializedMessage
        );
        try {
            final var kafkaServer = kafkaServerParam == null ? this.defaultKafkaServer : kafkaServerParam;
            final var producer = (Producer<Long, Object>) prepareProducer(kafkaServer, topic, serializeMode);
            ProducerRecord<Long, Object> record;
            record = new ProducerRecord<>(
                    topic,
                    partition,
                    null,
                    serializedMessage
            );

            //noinspection SynchronizationOnLocalVariableOrMethodParameter
            synchronized (producer) {
                // Собственно отправка в Kafka:
                final var recordMetadata = producer.send(record).get();
                log.info("FINISHED sendMessage(): offset = " + recordMetadata.offset());
                return new SendMessageResult(
                        kafkaServer,
                        recordMetadata.topic(),
                        recordMetadata.partition(),
                        recordMetadata.offset()
                );
            }
        } catch (Exception e) {
            log.info("FINISHED sendMessage() with ERROR: " + e.getMessage());
            throw e;
        }
    }
}
