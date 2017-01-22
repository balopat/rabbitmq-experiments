package com.balopat.distributedexperiments.rabbitmq;

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

/**
 * Created by balopat on 1/4/17.
 */
public abstract class ExperimentWorker implements Runnable {

    protected static final Logger LOG = LoggerFactory.getLogger(ExperimentWorker.class);
    protected final ExperimentConfig config;
    protected State state = State.INITIALIZING;
    private Throwable error = null;
    protected Connection connection;
    protected Channel channel;
    private final String name;


    public Throwable getError() {
        return error;
    }


    public ExperimentWorker(ExperimentConfig config, String name) {
        this.config = config;
        this.name = name;
    }


    @Override
    public void run() {
        try {
            runUnsafe();
        } catch (Throwable e) {
            LOG.error(this.name + " failed: ", e);
            state = State.FAILED;
            error = e;
        } finally {
            if (connection != null) try {
                connection.close();
            } catch (IOException e) {
                LOG.error(this.name + " failed to close connection", e);
            }
        }
    }

    protected abstract void runUnsafe() throws Throwable;

    public State getState() {
        return state;
    }

    private void connect() throws IOException, TimeoutException {
        ConnectionFactory connectionFactory = getConnectionFactory();
        connection = connectionFactory.newConnection();
        channel = connection.createChannel();
    }

    protected ConnectionFactory getConnectionFactory() {
        return new ConnectionFactory();
    }

    protected void withRetryingConnections(Runnable action) throws IOException, TimeoutException, InterruptedException {
        int retries = 0;
        boolean success = false;
        while (!success) {
            try {
                if (connectionIsHealthy()) {
                    action.run();
                    success = true;
                } else {
                    cleanupConnectionIfNeeded();
                    connect();
                    LOG.info(this.name + " successfully (re)connected!");
                    action.run();
                    success = true;
                }
            } catch (KillWorkerException e) {
                throw e;
            } catch (Exception e) {
                success = false;
                LOG.info(this.name + " could not connect yet! " + e + " retries: " + retries);
                if (connection != null) {
                    try {
                        connection.close();
                    } catch (Throwable ignored) {}
                }
                Thread.sleep(1000);
                retries++;
            }
        }
    }


    private boolean connectionIsHealthy() {
        return connection != null && connection.isOpen() && channel != null && channel.isOpen();
    }

    private void cleanupConnectionIfNeeded() {
        if (connection != null) {
            try {
                connection.close();
            } catch (Throwable ignored) {
            }
        }
    }

    public String getName() {
        return name;
    }


    public enum State {
        INITIALIZING, RUNNING, FINISHED, PAUSED, FAILED
    }

    public static class ExperimentConfig {
        final long sampleSize;
        final boolean cleanup;
        final boolean partitioning;
        final long consumerDeadlineAfterPublisherIsDone;

        public ExperimentConfig(long consumerDeadlineAfterPublisherIsDone, long sampleSize, boolean cleanup, boolean partitioning) {
            this.consumerDeadlineAfterPublisherIsDone = consumerDeadlineAfterPublisherIsDone;
            this.sampleSize = sampleSize;
            this.cleanup = cleanup;
            this.partitioning = partitioning;
        }

        @Override
        public String toString() {
            return "ExperimentConfig{" +
                    "sampleSize=" + sampleSize +
                    ", cleanup=" + cleanup +
                    ", partitioning=" + partitioning +
                    ", consumerDeadlineAfterPublisherIsDone=" + consumerDeadlineAfterPublisherIsDone +
                    '}';
        }
    }
}
