package com.balopat.distributedexperiments.rabbitmq;

import com.rabbitmq.client.*;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.TimeoutException;


public class CountingPublisher extends ExperimentWorker {


    private int port;
    private int startInterval;
    private int endInterval;

    public CountingPublisher(int port, int startInterval, int endInterval, ExperimentConfig config, PartitioningExperiment.ExperimentData data) {
        super(config, data, "CountingPublisher [" + port + "]");
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
            for (int i = startInterval; i < endInterval + 1; i++) {
                final byte[] bytes = Integer.toString(i).getBytes();
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
