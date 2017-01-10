package com.balopat.distributedexperiments.rabbitmq;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.TimeoutException;


public class CountingPublisher extends ExperimentWorker {


    private int port;
    private long startInterval;
    private long endInterval;

    public CountingPublisher(int port, long startInterval, long endInterval, ExperimentConfig config) {
        super(config, "CountingPublisher [" + port + "]");
        this.port = port;
        this.startInterval = startInterval;
        this.endInterval = endInterval;
    }

    @Override
    protected ConnectionFactory getConnectionFactory() {
        ConnectionFactory cf = new ConnectionFactory();
        cf.setPort(port);
        return cf;
    }

    @Override
    protected void runUnsafe() throws Throwable {
            state = State.RUNNING;
            for (long i = startInterval; i < endInterval + 1; i++) {
                final byte[] bytes = Long.toString(i).getBytes();
                withRetryingConnections(() -> {
                    try {
                        channel.confirmSelect();
                        channel.basicPublish("amq.fanout", "", true, false, new AMQP.BasicProperties.Builder().build(), bytes);
                        channel.waitForConfirmsOrDie(1000);
                    } catch (IOException | InterruptedException | TimeoutException e) {
                        throw new IllegalStateException(e);
                    }
                });
            }
            state = State.FINISHED;


    }


}
