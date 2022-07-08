package ru.gx.sender.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import ru.gx.core.channels.SerializeMode;
import ru.gx.core.messaging.MessageHeader;
import ru.gx.core.messaging.MessageKind;
import ru.gx.core.messaging.MessageRaw;
import ru.gx.sender.kafka.KafkaService;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.OffsetDateTime;
import java.util.UUID;

@Slf4j
@RestController
@RequiredArgsConstructor
public class Controller {
    private final static String POST_SEND_MESSAGE_BODY_DATA = "/send-message-body-data";
    private final static String POST_SEND_MESSAGE_JSON = "/send-message-json";

    private final static String SOURCE_SYSTEM = "core-kafka-sender-util";

    @NotNull
    private final ObjectMapper objectMapper;

    @NotNull
    private final KafkaService kafkaService;

    @SuppressWarnings("unused")
    @PostMapping(POST_SEND_MESSAGE_BODY_DATA)
    @Nullable
    public ResponseEntity<String> postSendMessageBodyData(
            @RequestParam(name = "kafkaServer", required = false) @Nullable final String kafkaServer,
            @RequestParam(name = "topic") @NotNull final String topic,
            @RequestParam(name = "partition", required = false) @Nullable final Integer partition,
            @RequestParam(name = "id", required = false) @Nullable final String id,
            @RequestParam(name = "parentId", required = false) @Nullable final String parentId,
            @RequestParam(name = "kind") @NotNull final String kind,
            @RequestParam(name = "type") @NotNull final String type,
            @RequestParam(name = "version", required = false) @Nullable final Integer version,
            @RequestParam(name = "serializeMode", required = false) @Nullable final String serializeModeString,
            @RequestParam(name = "sourceSystem", required = false, defaultValue = SOURCE_SYSTEM) @Nullable final String sourceSystem,
            @RequestBody @NotNull final String data
    ) {
        log.info("STARTED postSendMessageBody()");
        try {
            final var message = new MessageRaw(
                    new MessageHeader(
                            id != null ? id : UUID.randomUUID().toString(),
                            parentId,
                            MessageKind.valueOf(kind),
                            type,
                            version != null ? version : 1,
                            sourceSystem != null ? sourceSystem : SOURCE_SYSTEM,
                            OffsetDateTime.now()
                    ),
                    new MessageRaw.Body(data),
                    null
            );
            final var serializeMode = serializeModeString == null
                    ? SerializeMode.JsonString
                    : SerializeMode.valueOf(serializeModeString);
            final var serializedMessage = serializeMode == SerializeMode.JsonString
                    ? this.objectMapper.writeValueAsString(message)
                    : this.objectMapper.writeValueAsBytes(message);
            final var result = this.kafkaService.sendSerializedMessage(
                    kafkaServer,
                    topic,
                    partition == null ? 0 : partition,
                    serializeMode,
                    serializedMessage
            );
            log.info("FINISHED postSendMessageBody()");
            return ResponseEntity.ok().body(this.objectMapper.writeValueAsString(result));
        } catch (Exception e) {
            log.error("", e);
            final var sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            return ResponseEntity.badRequest().body(
                    e.getMessage() + "\n"
                            + sw
            );
        }
    }


    @SuppressWarnings("unused")
    @PostMapping(POST_SEND_MESSAGE_JSON)
    @Nullable
    public ResponseEntity<String> postSendMessageJson(
            @RequestParam(name = "kafkaServer", required = false) @Nullable final String kafkaServer,
            @RequestParam(name = "topic") @NotNull final String topic,
            @RequestParam(name = "partition", required = false) @Nullable final Integer partition,
            @RequestBody @NotNull final String jsonMessage
    ) {
        log.info("STARTED postSendMessageJson()");
        try {
            final var result = this.kafkaService.sendSerializedMessage(
                    kafkaServer,
                    topic,
                    partition == null ? 0 : partition,
                    SerializeMode.JsonString,
                    jsonMessage
            );
            log.info("FINISHED postSendMessageJson()");
            return ResponseEntity.ok().body(this.objectMapper.writeValueAsString(result));
        } catch (Exception e) {
            log.error("", e);
            final var sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            return ResponseEntity.badRequest().body(
                    e.getMessage() + "\n"
                            + sw
            );
        }
    }
}
