package com.balopat.distributedexperiments.rabbitmq;

import java.io.IOException;

import static com.balopat.distributedexperiments.rabbitmq.ExperimentWorker.State.FINISHED;
import static com.balopat.distributedexperiments.rabbitmq.ExperimentWorker.State.RUNNING;

public class PartitioningExperiment {

    public static class ExperimentData {

        private boolean finished;

        public void store() {

        }

        public boolean isFinished() {
            return finished;
        }
    }

    public static void main(String[] args) throws Exception {
        RabbitMQClusterManager rabbitMQClusterManager = null;
        try {
            rabbitMQClusterManager = new RabbitMQClusterManager();
            rabbitMQClusterManager.bringUpCluster();
            DockerPartitioner dockerPartitioner = new DockerPartitioner();
            ExperimentWorker.ExperimentConfig config = new ExperimentWorker.ExperimentConfig(100000, 10000);
            ExperimentData experimentData = runExperiment(config);
            System.out.println("Experiment finished.");
            experimentData.store();
        } finally {
            rabbitMQClusterManager.cleanup();
        }
    }

    private static ExperimentData runExperiment(ExperimentWorker.ExperimentConfig config) throws InterruptedException {
        ExperimentData experimentData = new ExperimentData();
        CountingConsumer countingConsumer = setupAndStartConsumer(config, experimentData);
        CountingPublisher countingPublisher = setupAndStartPublisher(config, experimentData);
        while (countingConsumer.getState() == RUNNING || countingPublisher.getState() == RUNNING) {
            Thread.sleep(1000);
            if (countingPublisher.getState() == FINISHED && !countingConsumer.hasDeadlineToFinish()) {
                countingConsumer.setDeadLineToFinish(config.consumerDeadlineAfterPublisherIsDone);
            }
        }
        return experimentData;
    }

    private static CountingConsumer setupAndStartConsumer(ExperimentWorker.ExperimentConfig config, ExperimentData experimentData) throws InterruptedException {
        CountingConsumer consumer = new CountingConsumer(config, experimentData);
        Thread consumerThread = new Thread(consumer);
        consumerThread.start();
        int healthCheckLimit = 3;
        while (healthCheckLimit > 0 && consumer.getState() != RUNNING) {
            Thread.sleep(1000);
            healthCheckLimit--;
        }
        if (consumer.getState() != RUNNING) {
            System.out.println("Consumer couldn't start, something's wrong: state: " + consumer.getState() + " error: " + consumer.getError());
        }
        return consumer;
    }

    private static CountingPublisher setupAndStartPublisher(ExperimentWorker.ExperimentConfig config, ExperimentData experimentData) throws InterruptedException {
        CountingPublisher publisher = new CountingPublisher(config, experimentData);
        Thread publisherThread = new Thread(publisher);
        publisherThread.start();
        int healthCheckLimit = 3;
        while (healthCheckLimit > 0 && publisher.getState() != RUNNING) {
            Thread.sleep(1000);
            healthCheckLimit--;
        }
        if (publisher.getState() != RUNNING) {
            System.out.println("Publisher couldn't start, something's wrong: state: " + publisher.getState() + " error: " + publisher.getError());
        }
        return publisher;
    }

}
