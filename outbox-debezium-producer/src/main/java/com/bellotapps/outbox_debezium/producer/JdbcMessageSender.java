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
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static com.bellotapps.outbox_debezium.commons.MessageFields.*;


/**
 * Created by Juan Marcos Bellini on 2019-04-19.
 */
public class JdbcMessageSender implements MessageSender {

    /**
     * The {@link Logger}.
     */
    private final static Logger LOGGER = LoggerFactory.getLogger(JdbcMessageSender.class);

    /**
     * The outbox publish sql query pattern (i.e can be customized with the schema and table name of the outbox).
     */
    private final static String SQL_PATTERN =
            "INSERT INTO {0}.{1} ({2}, {3}, {4}, {5}, {6}, {7}) VALUES (?, ?, ?, ?, ?, ?);";

    /**
     * The SQL query.
     */
    private final String sql;

    /**
     * A {@link Supplier} of {@link Connection}
     * (i.e used when executing the {@link #send(Message, String)} method, in order to get a {@link Connection}
     * in which the transaction in which the outbox publish is happening).
     */
    private final Supplier<Connection> connectionSupplier;

    /**
     * Constructor.
     *
     * @param schema             The schema in which the outbox table resides.
     * @param table              The outbox table.
     * @param connectionSupplier A {@link Supplier} of {@link Connection}.
     *                           Must return a {@link Connection} in which the transaction
     *                           in which the outbox publish is happening.
     */
    public JdbcMessageSender(final String schema, final String table, final Supplier<Connection> connectionSupplier) {
        this.connectionSupplier = connectionSupplier;
        this.sql = MessageFormat
                .format(SQL_PATTERN, schema, table, MESSAGE_ID, SENDER, RECIPIENT, TIMESTAMP, HEADERS, PAYLOAD);


    }

    @Override
    public void send(final Message message, final String recipient) {
        final Connection connection = connectionSupplier.get();
        try (final PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            configurePreparedStatement(preparedStatement, message, recipient);
            preparedStatement.executeUpdate();
        } catch (final SQLException e) {
            LOGGER.error("Could not publish message in the outbox table.");
            LOGGER.debug("SQLException message: {}", e.getMessage());
            LOGGER.trace("Stacktrace: ", e);
        }
    }

    /**
     * Configures the given {@code preparedStatement} using data from the given {@code message},
     * and with the given {@code recipient}.
     *
     * @param preparedStatement The {@link PreparedStatement} to be configured.
     * @param message           The {@link Message} from where data is taken.
     * @param recipient         The recipient (is stored in the database).
     */
    private static void configurePreparedStatement(
            final PreparedStatement preparedStatement,
            final Message message,
            final String recipient) {
        final String headers = message.getHeaders().entrySet()
                .stream()
                .map(e -> e.getKey() + ": " + e.getValue())
                .collect(Collectors.joining("\n"));
        final Optional<String> payloadOptional = message.getPayload();
        try {
            preparedStatement.setString(1, message.getId());
            preparedStatement.setString(2, message.getSender());
            preparedStatement.setString(3, recipient);
            preparedStatement.setLong(4, message.getTimestamp());
            preparedStatement.setString(5, StringUtils.isBlank(headers) ? null : headers);
            preparedStatement.setString(6, payloadOptional.orElse(null));
        } catch (final SQLException e) {
            LOGGER.error("Could not configure the prepared statement");
            LOGGER.debug("SQLException message: {}", e.getMessage());
            LOGGER.trace("Stacktrace: ", e);
        }
    }
}
