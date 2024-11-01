package com.p14n.postevent;

import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;
import java.util.function.Consumer;

public class DebeziumEngineWrapper implements Closeable {
    private final Properties properties;
    private DebeziumEngine<SourceRecord> engine;
    private Consumer<SourceRecord> recordConsumer;

    public DebeziumEngineWrapper(Properties properties) {
        this.properties = properties;
    }

    public void setRecordConsumer(Consumer<SourceRecord> consumer) {
        this.recordConsumer = consumer;
    }

    public void start() {
        if (recordConsumer == null) {
            throw new IllegalStateException("Record consumer must be set before starting the engine");
        }

        this.engine = DebeziumEngine.create(Json.class)
                .using(properties)
                .notifying(recordConsumer)
                .build();

        Thread engineThread = new Thread(() -> {
            try {
                engine.run();
            } catch (IOException e) {
                throw new RuntimeException("Failed to run Debezium engine", e);
            }
        });
        engineThread.start();
    }

    @Override
    public void close() throws IOException {
        if (engine != null) {
            engine.close();
        }
    }
}
package com.p14n.postevent;

import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;
import java.util.function.Consumer;

public class DebeziumEngineWrapper implements Closeable {
    private final Properties properties;
    private DebeziumEngine<?> engine;
    private Consumer<SourceRecord> recordConsumer;

    public DebeziumEngineWrapper(Properties properties) {
        this.properties = properties;
    }

    public void setRecordConsumer(Consumer<SourceRecord> consumer) {
        this.recordConsumer = consumer;
    }

    public void start() {
        if (recordConsumer == null) {
            throw new IllegalStateException("Record consumer must be set before starting the engine");
        }

        this.engine = DebeziumEngine.create(Json.class)
                .using(properties)
                .notifying(record -> {
                    if (record instanceof SourceRecord) {
                        recordConsumer.accept((SourceRecord) record);
                    }
                })
                .build();

        Thread thread = new Thread(() -> {
            try {
                engine.run();
            } catch (IOException e) {
                throw new RuntimeException("Failed to run Debezium engine", e);
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
package com.p14n.postevent;

import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.format.Json;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;
import java.util.function.Consumer;

public class DebeziumEngineWrapper implements Closeable {
    private final Properties properties;
    private Consumer<SourceRecord> recordConsumer;
    private DebeziumEngine<SourceRecord> engine;

    public DebeziumEngineWrapper(Properties properties) {
        this.properties = properties;
    }

    public void setRecordConsumer(Consumer<SourceRecord> recordConsumer) {
        this.recordConsumer = recordConsumer;
    }

    public void start() {
        if (recordConsumer == null) {
            throw new IllegalStateException("Record consumer must be set before starting the engine");
        }

        this.engine = DebeziumEngine.create(SourceRecord.class)
                .using(properties)
                .notifying(recordConsumer)
                .build();

        Thread thread = new Thread(() -> {
            try {
                engine.run();
            } catch (IOException e) {
                throw new RuntimeException("Failed to run Debezium engine", e);
            }
        });
        thread.setName("debezium-engine");
        thread.start();
    }

    @Override
    public void close() throws IOException {
        if (engine != null) {
            engine.close();
        }
    }
}
