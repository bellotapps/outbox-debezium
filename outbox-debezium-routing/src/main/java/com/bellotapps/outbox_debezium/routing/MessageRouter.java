/*
 * Copyright 2019 BellotApps
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bellotapps.outbox_debezium.routing;

import com.bellotapps.outbox_debezium.commons.Message;
import com.bellotapps.outbox_debezium.commons.MessageFields;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;


/**
 * Re-routes records from the outbox table to the topic indicated by the {@link MessageFields#RECIPIENT} field
 * of the "after" data in the {@link ConnectRecord} to be processed.
 */
public class MessageRouter<R extends ConnectRecord<R>> implements Transformation<R> {

    /**
     * A {@link Logger}.
     */
    private final static Logger LOGGER = LoggerFactory.getLogger(MessageRouter.class);


    @Override
    public void configure(final Map<String, ?> configs) {
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void close() {
    }


    @Override
    public R apply(final R record) {

        // Ignore tombstones.
        if (record.value() == null) {
            return record;
        }

        if (!record.valueSchema().type().equals(Schema.Type.STRUCT)) {
            LOGGER.warn("Received a record that is not a struct. Skipping record!");
            return null;
        }

        final Struct recordStruct = (Struct) record.value();
        try {
            final String operation = recordStruct.getString("op");

            // First check if the operation is a create. This is the only operation that we care.
            if (!"c".equals(operation)) {
                // Operation is not a create. Check what it is.
                // Updates and deletes are part of debezium, but we don't care about them in the outbox pattern.
                if (!("u".equals(operation) || "d".equals(operation))) {
                    // If it is not a create, update, or delete, then the message does not comply with the protocol.
                    LOGGER.warn("Received an unknown operation. Skipping record!");
                }
                return null;
            }

            final Struct data = recordStruct.getStruct("after");
            final Optional<String> topicOptional = getTopicFromData(data);
            if (!topicOptional.isPresent()) {
                LOGGER.warn("Skipping record as topic could not be gotten from data");
                return null;
            }
            final Optional<Message> messageOptional = buildMessageFromData(data);
            if (!messageOptional.isPresent()) {
                LOGGER.warn("Skipping record as message could not be created from data");
                return null;
            }

            final String topic = topicOptional.get();
            final Message message = messageOptional.get();

            final Schema messageSchema = buildMessageSchema(message);
            final Struct messageStruct = buildMessageStruct(message, messageSchema);

            return record.newRecord(
                    topic,
                    record.kafkaPartition(),
                    Schema.STRING_SCHEMA,
                    "message",
                    messageSchema,
                    messageStruct,
                    record.timestamp()
            );
        } catch (final DataException e) {
            LOGGER.warn("Missing or invalid \"op\" or \"after\" fields. Skipping record!");
            logErrorAndStacktrace(e);
            return null;
        } catch (final Throwable e) {
            logUnexpectedException(e);
            return null;
        }
    }


    /**
     * Analyzes the given {@code data} for the kafka topic.
     *
     * @param data A {@link Struct} from where the topic will be taken.
     * @return An {@link Optional} with the topic if the given {@link Struct}
     * contains a field named {@link MessageFields#RECIPIENT} whose schema is {@link Schema#STRING_SCHEMA},
     * or empty otherwise.
     */
    private static Optional<String> getTopicFromData(final Struct data) {
        try {
            final String topic = data.getString(MessageFields.RECIPIENT) + "-Messages";
            return Optional.of(topic);
        } catch (final DataException e) {
            LOGGER.warn("Missing recipient, or recipient has invalid type. Skipping record!");
            return Optional.empty();
        } catch (final Throwable e) {
            logUnexpectedException(e);
            return Optional.empty();
        }
    }

    /**
     * Builds a {@link Message} from the given {@code data}.
     *
     * @param data A {@link Struct} from where message data will be taken.
     * @return An {@link Optional} with the {@link Message} if all needed data is available (with corresponding type),
     * or empty otherwise.
     */
    private static Optional<Message> buildMessageFromData(final Struct data) {
        try {
            // First, create the builder with the mandatory fields (id, sender and timestamp).
            final Message.Builder messageBuilder = Message.Builder.create()
                    .withId(data.getString(MessageFields.MESSAGE_ID))
                    .from(data.getString(MessageFields.SENDER))
                    .at(data.getInt64(MessageFields.TIMESTAMP));
            // Then check if there are headers present
            if (data.schema().field(MessageFields.HEADERS) != null) {
                final String headers = data.getString(MessageFields.HEADERS);
                final Map<String, String> headersMap = Arrays.stream(StringUtils.split(headers, "\n"))
                        .map(str -> StringUtils.split(str, ":", 2))
                        .filter(arr -> arr.length == 2) // Only headers with key and value
                        .peek(arr -> arr[0] = arr[0].trim()) // Trim key
                        .peek(arr -> arr[1] = arr[1].trim()) // Trim value
                        .collect(Collectors.toMap(arr -> arr[0], arr -> arr[1]));
                messageBuilder.withHeaders(headersMap);
            }
            // Then check if there is a payload present
            if (data.schema().field(MessageFields.PAYLOAD) != null) {
                messageBuilder.withPayload(data.getString(MessageFields.PAYLOAD));
            }
            // Finally, build the message.
            return Optional.of(messageBuilder.build());
        } catch (final IllegalArgumentException | DataException e) {
            LOGGER.warn("Missing or invalid data. Skipping record!");
            logErrorAndStacktrace(e);
            return Optional.empty();
        } catch (final Throwable e) {
            logUnexpectedException(e);
            return Optional.empty();
        }
    }

    /**
     * Builds a {@link Struct} {@link Schema} for a {@link Message}.
     *
     * @param message The {@link Message} from which the {@link Schema} will be built.
     * @return The created {@link Schema}.
     */
    private static Schema buildMessageSchema(final Message message) {
        final SchemaBuilder messageSchemaBuilder = SchemaBuilder.struct()
                .field(MessageFields.MESSAGE_ID, Schema.STRING_SCHEMA)
                .field(MessageFields.SENDER, Schema.STRING_SCHEMA)
                .field(MessageFields.TIMESTAMP, Schema.INT64_SCHEMA);
        message.getHeaders().keySet().forEach(key -> messageSchemaBuilder.field(key, Schema.STRING_SCHEMA));
        message.getPayload().ifPresent(ign -> messageSchemaBuilder.field(MessageFields.PAYLOAD, Schema.STRING_SCHEMA));
        return messageSchemaBuilder.build();
    }

    /**
     * Builds a {@link Struct} of {@link Message}.
     *
     * @param message       The {@link Message} from where data is taken.
     * @param messageSchema The {@link Schema} used to build the {@link Struct}.
     * @return The created {@link Struct}.
     */
    private static Struct buildMessageStruct(final Message message, final Schema messageSchema) {
        final Struct messageStruct = new Struct(messageSchema)
                .put(MessageFields.MESSAGE_ID, message.getId())
                .put(MessageFields.SENDER, message.getSender())
                .put(MessageFields.TIMESTAMP, message.getTimestamp());
        message.getHeaders().forEach(messageStruct::put);
        message.getPayload().ifPresent(payload -> messageStruct.put(MessageFields.PAYLOAD, payload));
        return messageStruct;
    }

    /**
     * Logs an unexpected exception.
     *
     * @param e The {@link Throwable} that was unexpected.
     */
    private static void logUnexpectedException(final Throwable e) {
        LOGGER.error("Unexpected error. Skipping record!");
        logErrorAndStacktrace(e);
    }

    /**
     * Logs the given {@link Throwable}'s message and stacktrace.
     *
     * @param e The {@link Throwable} that was unexpected.
     */
    private static void logErrorAndStacktrace(final Throwable e) {
        LOGGER.debug("Exception message: {}", e.getMessage());
        LOGGER.trace("Stacktrace: ", e);
    }
}
