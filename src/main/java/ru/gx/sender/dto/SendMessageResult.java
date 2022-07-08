package ru.gx.sender.dto;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@RequiredArgsConstructor
public class SendMessageResult {
    private final String kafkaServer;

    private final String topic;

    private final int partition;

    private final long offset;
}
