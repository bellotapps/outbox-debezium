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

package com.bellotapps.outbox_debezium.commons;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;

import java.time.Instant;
import java.util.*;
import java.util.function.Supplier;

/**
 * Represents a message.
 */
public class Message {

    /**
     * The message's id.
     */
    private final String id;

    /**
     * An identification of the sender.
     * This allows the recipient to know who has sent the message.
     */
    private final String sender;

    /**
     * The timestamp of the message.
     */
    private final Instant timestamp;

    /**
     * The message headers.
     */
    private final Map<String, String> headers;

    /**
     * The message payload.
     */
    private final String payload;

    /**
     * Private constructor. Use
     *
     * @param id        The message's id.
     * @param sender    An identification of the sender. This allows the recipient to know who has sent the message.
     * @param timestamp The timestamp of the message.
     * @param headers   The message headers.
     * @param payload   The message payload.
     * @throws IllegalArgumentException If any argument is invalid.
     */
    private Message(
            final String id,
            final String sender,
            final Instant timestamp,
            final Map<String, String> headers,
            final String payload) throws IllegalArgumentException {
        Validate.isTrue(StringUtils.isNotBlank(id), "The id must have text");
        Validate.isTrue(StringUtils.isNotBlank(sender), "The sender must have text");
        Validate.notNull(timestamp, "The timestamp must not be null");
        Validate.notNull(headers, "The headers map must not be null");
        Validate.isTrue(
                headers.entrySet().stream().noneMatch(e -> StringUtils.isAnyBlank(e.getKey(), e.getValue())),
                "All the headers key and value must have text"
        );
        this.id = id;
        this.sender = sender;
        this.timestamp = timestamp;
        this.headers = Collections.unmodifiableMap(headers);
        this.payload = payload;
    }

    /**
     * @return The message's id.
     */
    public String getId() {
        return id;
    }

    /**
     * @return An identification of the sender. This allows the recipient to know who has sent the message.
     */
    public String getSender() {
        return sender;
    }

    /**
     * @return The timestamp of the message.
     */
    public Instant getTimestamp() {
        return timestamp;
    }

    /**
     * @return The message headers.
     */
    public Map<String, String> getHeaders() {
        return headers;
    }

    /**
     * @return An {@link Optional} containing the payload if there is such, or empty otherwise.
     */
    public Optional<String> getPayload() {
        return Optional.ofNullable(payload);
    }


    /**
     * A builder of {@link Message}s.
     */
    public static class Builder {

        /**
         * The id of the {@link Message}.
         */
        private String id;
        /**
         * The sender of the {@link Message}.
         */
        private String sender;
        /**
         * The timestamp of the {@link Message}.
         */
        private Supplier<Instant> timestampSupplier;
        /**
         * The headers of the {@link Message}.
         */
        private final Map<String, String> headers;
        /**
         * The payload of the {@link Message}.
         */
        private String payload;

        /**
         * Constructor.
         */
        private Builder() {
            this.headers = new HashMap<>();
        }

        /**
         * Sets the id of the {@link Message} to be built.
         *
         * @param id The id of the {@link Message}.
         * @return {@code this} for method chaining.
         */
        public Builder withId(final String id) {
            this.id = id;
            return this;
        }

        /**
         * Sets a random id to the {@link Message} to be built.
         *
         * @return {@code this} for method chaining.
         * @implNote This methods uses {@link UUID#randomUUID()} for random id generation.
         */
        public Builder withRandomId() {
            return withId(UUID.randomUUID().toString());
        }

        /**
         * Sets the sender of the {@link Message} to be built.
         *
         * @param sender The sender of the {@link Message}.
         * @return {@code this} for method chaining.
         */
        public Builder from(final String sender) {
            this.sender = sender;
            return this;
        }

        /**
         * Sets the timestamp of the {@link Message} to be built to now.
         *
         * @return {@code this} for method chaining.
         */
        public Builder atNow() {
            return at(Instant.now());
        }

        /**
         * Sets the timestamp of the {@link Message} to be built.
         *
         * @param timestamp The {@link Instant} representing the timestamp of the {@link Message}.
         * @return {@code this} for method chaining.
         */
        public Builder at(final Instant timestamp) {
            return atSupplied(() -> timestamp);
        }


        /**
         * Makes the builder use {@link Instant#now()} as a {@link Supplier} of {@link Instant}.
         * This means that the {@link Message} will have as a timestamp the moment it is created
         * by the {@link #build()} method.
         *
         * @return {@code this} for method chaining.
         */
        public Builder atBuildTime() {
            return atSupplied(Instant::now);
        }

        /**
         * Sets a {@link Supplier} of {@link Instant} to be used to generate the timestamp of the {@link Message}
         * to be built. The {@link Supplier#get()} method will be executed when the {@link #build()} method is executed.
         *
         * @param timestampSupplier The {@link Supplier} of {@link Instant} to be used.
         * @return {@code this} for method chaining.
         */
        public Builder atSupplied(final Supplier<Instant> timestampSupplier) {
            this.timestampSupplier = timestampSupplier;
            return this;
        }


        /**
         * Replaces all the headers in this builders with the given {@code headers}.
         *
         * @param headers The new headers {@link Map}.
         * @return {@code this} for method chaining.
         * @apiNote This method replaces the header if the key is already in use.
         */
        public Builder replaceHeaders(final Map<String, String> headers) {
            this.headers.clear();
            this.headers.putAll(headers);
            return this;
        }

        /**
         * Adds all the given {@code headers}.
         *
         * @param headers The headers to be added.
         * @return {@code this} for method chaining.
         * @apiNote This operation overrides already set headers.
         */
        public Builder withHeaders(final Map<String, String> headers) {
            this.headers.putAll(headers);
            return this;
        }

        /**
         * Adds a header to the {@link Message} to be built.
         *
         * @param key   The header's key.
         * @param value The header's value.
         * @return {@code this} for method chaining.
         * @apiNote This method replaces the header if the key is already in use.
         */
        public Builder withHeader(final String key, final String value) {
            this.headers.put(key, value);
            return this;
        }

        /**
         * Removes the header with the given {@code key} from the {@link Message} to be built.
         *
         * @param key The header key to be removed.
         * @return {@code this} for method chaining.
         */
        public Builder withoutHeader(final String key) {
            this.headers.remove(key);
            return this;
        }

        /**
         * Removes all headers.
         *
         * @return {@code this} for method chaining.
         */
        public Builder clearHeaders() {
            this.headers.clear();
            return this;
        }

        /**
         * Sets the payload of the {@link Message} to be built.
         *
         * @param payload The payload of the {@link Message}.
         * @return {@code this} for method chaining.
         */
        public Builder withPayload(final String payload) {
            this.payload = payload;
            return this;
        }


        /**
         * Clears this builder.
         *
         * @return {@code this} for method chaining.
         */
        public Builder clear() {
            this.id = null;
            this.sender = null;
            this.timestampSupplier = null;
            this.headers.clear();
            this.payload = null;

            return this;
        }

        /**
         * Creates an instance of {@link Message} according to this builder configuration.
         * Future modifications to this builder won't affect the returned {@link Message}.
         *
         * @return An instance of {@link Message}.
         * @throws IllegalArgumentException If any argument is invalid.
         */
        public Message build() throws IllegalArgumentException {
            if (id == null) {
                withRandomId();
            }
            if (timestampSupplier == null) {
                atBuildTime();
            }
            return new Message(id, sender, timestampSupplier.get(), headers, payload);
        }

        /**
         * Creates a new builder instance.
         *
         * @return A new builder instance.
         */
        public static Builder create() {
            return new Builder();
        }
    }
}
