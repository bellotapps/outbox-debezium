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

/**
 * Class defining message's fields as constants.
 */
public class MessageFields {

    /**
     * The message's id field name.
     */
    public static final String MESSAGE_ID = "id";
    /**
     * The message's sender field name.
     */
    public static final String SENDER = "sender";
    /**
     * The message's recipient field name.
     */
    public static final String RECIPIENT = "recipient";
    /**
     * The message's timestamp field name.
     */
    public static final String TIMESTAMP = "timestamp";
    /**
     * The message's headers field name.
     */
    public static final String HEADERS = "headers";
    /**
     * The message's id payload name.
     */
    public static final String PAYLOAD = "payload";
}
