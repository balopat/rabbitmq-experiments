package com.balopat.distributedexperiments.rabbitmq;

import com.rabbitmq.client.*;


public class CountingPublisher extends ExperimentWorker {


    public CountingPublisher(ExperimentConfig config, PartitioningExperiment.ExperimentData data) {
        super(config, data);
    }

    @Override
    protected void runUnsafe() throws Throwable {
        Connection connection = null;
        try {
            ConnectionFactory connectionFactory = new ConnectionFactory();
            connection = connectionFactory.newConnection();
            Channel channel = connection.createChannel();
            state = State.RUNNING;
            for (int i = 0; i < config.sampleSize; i++) {
                channel.basicPublish("amq.fanout", "", true, false, new AMQP.BasicProperties.Builder().build(), Integer.toString(i).getBytes());
            }
            state = State.FINISHED;
        } finally {
            if (connection != null) connection.close();
        }

    }
}
