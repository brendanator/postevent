package com.p14n.postevent;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

public class PostgresDebeziumConnector implements Closeable {
    private final Properties properties;
    private Consumer<ChangeEvent<String, String>> changeEventConsumer;
    private DebeziumEngine<ChangeEvent<String, String>> engine;
    private ExecutorService executor;

    public PostgresDebeziumConnector(Properties properties) {
        this.properties = properties;
    }

    public void setChangeEventConsumer(Consumer<ChangeEvent<String, String>> consumer) {
        this.changeEventConsumer = consumer;
    }

    public void start() {
        if (changeEventConsumer == null) {
            throw new IllegalStateException("Change event consumer must be set before starting");
        }

        this.engine = DebeziumEngine.create(Json.class)
                .using(properties)
                .notifying(changeEventConsumer)
                .build();

        this.executor = Executors.newSingleThreadExecutor();
        this.executor.execute(engine);
    }

    @Override
    public void close() throws IOException {
        if (engine != null) {
            engine.close();
        }
        if (executor != null) {
            executor.shutdown();
        }
    }
}
