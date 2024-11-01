package com.p14n.postevent;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;
import java.util.function.Consumer;

public class PostgresDebeziumConnector implements Closeable {
    private final Properties properties;
    private Consumer<ChangeEvent<String, String>> changeEventConsumer;
    private DebeziumEngine<ChangeEvent<String, String>> engine;

    public PostgresDebeziumConnector(Properties properties) {
        this.properties = properties;
    }

    public void setChangeEventConsumer(Consumer<ChangeEvent<String, String>> consumer) {
        this.changeEventConsumer = consumer;
    }

    public void start() {
        this.engine = DebeziumEngine.create(Json.class)
                .using(properties)
                .notifying(changeEventConsumer)
                .build();

        // Start the engine in a separate thread
        Thread thread = new Thread(() -> {
            try {
                engine.run();
            } catch (IOException e) {
                throw new RuntimeException("Failed to start Debezium engine", e);
            }
        });
        thread.start();
    }

    @Override
    public void close() throws IOException {
        if (engine != null) {
            engine.close();
        }
    }
}
