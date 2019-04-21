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

package com.bellotapps.outbox_debezium.producer;

import com.bellotapps.outbox_debezium.commons.Message;

/**
 * Defines behaviour for an object that can publish messages in the outbox table.
 */
public interface MessageSender {

    /**
     * Sends the given {@code message} to the given {@code recipient}
     * (i.e it publish a message in the outbox table).
     *
     * @param message   The {@link Message} to be sent.
     * @param recipient The recipient of the {@link Message}.
     */
    void send(final Message message, final String recipient);
}
