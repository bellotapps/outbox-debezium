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

import com.bellotapps.outbox_debezium.commons.MessageFields;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.transforms.Transformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Optional;


/**
 * Re-routes records from the outbox table to the topic indicated by the {@link MessageFields#RECIPIENT} field
 * of the "after" data in the {@link ConnectRecord} to be processed.
 * <p>
 * This component works with Postgres.
 * Behaviour when being used with other database system is unknown.
 * Specially because of the timestamp data type serialization.
 * <p>
 * Check <a href="https://debezium.io/docs/connectors"/>Debezium's connectors documentation</a>
 * to check compatibilities, and to know how to create a new {@link Transformation} for other database systems.
 *
 * @see <a href="https://debezium.io/docs/">Debezium's Documentation</a>
 */
public class PostgresMessageRouter<R extends ConnectRecord<R>> implements Transformation<R> {

    /**
     * A {@link Logger}.
     */
    private final static Logger LOGGER = LoggerFactory.getLogger(PostgresMessageRouter.class);


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
            LOGGER.warn("Received a record that is not a Struct. Skipping record!");
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
            final Optional<Struct> structOptional = getStructFromData(data);
            if (!structOptional.isPresent()) {
                LOGGER.warn("Skipping record as message information could not be taken from data");
                return null;
            }

            final String topic = topicOptional.get();
            final Struct struct = structOptional.get();
            return record.newRecord(
                    topic,
                    record.kafkaPartition(),
                    Schema.STRING_SCHEMA,
                    "message",
                    struct.schema(),
                    struct,
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


    // ================================================================================================================
    // Helpers
    // ================================================================================================================

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
     * Creates a {@link Struct} with message data to be streamed from the given {@code data} {@link Struct}.
     *
     * @param data The {@link Struct} from where the message to be streamed data is taken.
     * @return An {@link Optional} with the created {@link Struct} if everything is good to go,
     * or empty if the given {@code data} has missing fields, or the types are not the expected.
     */
    private static Optional<Struct> getStructFromData(final Struct data) {
        try {
            // First check fields
            final Optional<Field> idOptional = Optional.ofNullable(data.schema().field(MessageFields.MESSAGE_ID));
            final Optional<Field> senderOptional = Optional.ofNullable(data.schema().field(MessageFields.SENDER));
            final Optional<Field> timestampOptional = Optional.ofNullable(data.schema().field(MessageFields.TIMESTAMP));
            final Optional<Field> headersOptional = Optional.ofNullable(data.schema().field(MessageFields.HEADERS));
            final Optional<Field> payloadOptional = Optional.ofNullable(data.schema().field(MessageFields.PAYLOAD));

            // Check if mandatory fields are present. Return an empty Optional if any is missing.
            if (!(idOptional.isPresent() && senderOptional.isPresent() && timestampOptional.isPresent())) {
                LOGGER.warn("Missing mandatory data. Skipping record!");
                return Optional.empty();
            }
            final Field id = idOptional.get();
            final Field sender = senderOptional.get();
            final Field timestamp = timestampOptional.get();


            // Up to now we know that the Schema and Struct must be created.
            // Start creating the schema with mandatory stuff.
            final SchemaBuilder schemaBuilder = SchemaBuilder.struct()
                    .field(MessageFields.MESSAGE_ID, id.schema())
                    .field(MessageFields.SENDER, sender.schema())
                    .field(MessageFields.TIMESTAMP, timestamp.schema());
            // Add headers and payload if needed
            headersOptional.ifPresent(field -> setUpField(schemaBuilder, field));
            payloadOptional.ifPresent(field -> setUpField(schemaBuilder, field));
            // Build the schema
            final Schema schema = schemaBuilder.build();


            // Now the struct must be created. This consists of copying data into the new Struct.
            final Struct struct = new Struct(schema);
            putField(struct, data, id);
            putField(struct, data, sender);
            putField(struct, data, timestamp);
            headersOptional.ifPresent(field -> putField(struct, data, field));
            payloadOptional.ifPresent(field -> putField(struct, data, field));

            // Now, return the new Struct.
            return Optional.of(struct);
        } catch (final DataException e) {
            LOGGER.warn("Invalid data. Skipping record!");
            logErrorAndStacktrace(e);
            return Optional.empty();
        } catch (final Throwable e) {
            logUnexpectedException(e);
            return Optional.empty();
        }
    }


    /**
     * Sets up the given {@code field} in the given {@code schemaBuilder}.
     *
     * @param schemaBuilder The {@link SchemaBuilder} being configured.
     * @param field         The {@link Field} being configured.
     */
    private static void setUpField(final SchemaBuilder schemaBuilder, final Field field) {
        schemaBuilder.field(field.name(), field.schema());
    }

    /**
     * Puts data taken from the {@code sourceStruct} {@link Struct} (belonging to the given {@code field},
     * into the given {@code destStruct}, in a {@link Field} that has the same name as the said one.
     *
     * @param destStruct   The {@link Struct} in which data is being put.
     * @param sourceStruct The {@link Struct} from where data is being taken.
     * @param field        The {@link Field} owning the data in the {@code sourceStruct}.
     *                     Its name will be used in the the {@code destStruct}.
     * @throws RuntimeException If the {@code sourceStruct} does not contain the given {@code field}.
     */
    private static void putField(final Struct destStruct, final Struct sourceStruct, final Field field)
            throws RuntimeException {
        final Object value = sourceStruct.get(field);
        if (value == null) {
            LOGGER.warn("The data Struct Schema contained a field that is not present in the said Struct");
            throw new IllegalArgumentException("The given field does not belong to the source Struct.");
        }
        destStruct.put(field.name(), value);
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
