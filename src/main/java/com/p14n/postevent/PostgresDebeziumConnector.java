package com.p14n.postevent;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;

import java.io.IOException;
import java.util.Properties;
import java.util.function.Consumer;

public class PostgresDebeziumConnector {
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
        if (changeEventConsumer == null) {
            throw new IllegalStateException("Change event consumer must be set before starting the engine");
        }

        this.engine = DebeziumEngine.create(Json.class)
                .using(properties)
                .notifying(changeEventConsumer)
                .build();

        var thread = new Thread(() -> {
            try {
                engine.run();
            } catch (IOException e) {
                throw new RuntimeException("Failed to start Debezium engine", e);
            }
        });
        thread.setName("debezium-engine");
        thread.start();
    }

    public void close() {
        if (engine != null) {
            try {
                engine.close();
            } catch (IOException e) {
                throw new RuntimeException("Failed to close Debezium engine", e);
            }
        }
    }
}
