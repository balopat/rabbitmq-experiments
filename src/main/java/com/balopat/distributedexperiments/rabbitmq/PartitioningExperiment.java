package com.balopat.distributedexperiments.rabbitmq;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

import static com.balopat.distributedexperiments.rabbitmq.DockerPartitioner.*;
import static com.balopat.distributedexperiments.rabbitmq.ExperimentWorker.State.FINISHED;
import static com.balopat.distributedexperiments.rabbitmq.ExperimentWorker.State.RUNNING;

public class PartitioningExperiment {

    private static final Logger LOG = LoggerFactory.getLogger(PartitioningExperiment.class);

    public static class ExperimentData {

        private boolean finished;

        public void store() {

        }

        public boolean isFinished() {
            return finished;
        }
    }

    public static void main(String[] args) throws Exception {
        System.out.println("running with args: " + Arrays.toString(args));
        RabbitMQClusterManager rabbitMQClusterManager = new RabbitMQClusterManager();
        try {
            rabbitMQClusterManager.bringUpCluster();
            DockerPartitioner dockerPartitioner = new DockerPartitioner();
            ExperimentWorker.ExperimentConfig config = new ExperimentWorker.ExperimentConfig(120000, 100000);
            ExperimentData experimentData = runExperiment(config, dockerPartitioner);
            System.out.println("Experiment finished.");
            experimentData.store();
        } finally {
            if (args.length == 0 || !args[0].equals("--nocleanup")) {
                rabbitMQClusterManager.cleanup();
            }
        }
    }

    private static ExperimentData runExperiment(ExperimentWorker.ExperimentConfig config, DockerPartitioner dockerPartitioner) throws InterruptedException {
        ExperimentData experimentData = new ExperimentData();
        CountingConsumer countingConsumer = setupAndStartConsumer(config, experimentData);
        CountingPublisher countingPublisher1 = setupAndStartPublisher(0, config.sampleSize / 2, config, experimentData);
        CountingPublisher countingPublisher2 = setupAndStartPublisher(config.sampleSize / 2 + 1, config.sampleSize - 1, config, experimentData);
        new Thread(() -> {
            LOG.info("partitioning away rabbit3 from rabbit1 and rabbit2, sleep 6s...");
            dockerPartitioner.partitionAway(RABBIT3, RABBIT1, RABBIT2);
            try {
                Thread.sleep(6000);
            } catch (InterruptedException e) {}

            LOG.info("healing rabbit3");
            dockerPartitioner.healNetwork(RABBIT3);
        });
        while (countingConsumer.getState() == RUNNING
                || countingPublisher1.getState() == RUNNING
                || countingPublisher2.getState() == RUNNING) {
            Thread.sleep(1000);
            if (countingPublisher1.getState() == FINISHED
                    && countingPublisher2.getState() == FINISHED
                    && !countingConsumer.hasDeadlineToFinish()) {
                countingConsumer.setDeadLineToFinish(config.consumerDeadlineAfterPublisherIsDone);
            }
        }
        return experimentData;
    }

    private static CountingConsumer setupAndStartConsumer(ExperimentWorker.ExperimentConfig config, ExperimentData experimentData) throws InterruptedException {
        CountingConsumer consumer = new CountingConsumer(config, experimentData);
        Thread consumerThread = new Thread(consumer);
        consumerThread.start();
        int healthCheckLimit = 10;
        while (healthCheckLimit > 0 && consumer.getState() != RUNNING) {
            Thread.sleep(1000);
            healthCheckLimit--;
        }
        if (consumer.getState() != RUNNING && consumer.getState() != FINISHED) {
            System.out.println("Consumer couldn't start, something's wrong: state: " + consumer.getState() + " error: " + consumer.getError());
        }
        return consumer;
    }

    private static CountingPublisher setupAndStartPublisher(int startInterval, int endInterval, ExperimentWorker.ExperimentConfig config, ExperimentData experimentData) throws InterruptedException {
        CountingPublisher publisher = new CountingPublisher(RabbitMQClusterManager.RABBIT2_PORT, startInterval, endInterval, config, experimentData);
        Thread publisherThread = new Thread(publisher);
        publisherThread.start();
        int healthCheckLimit = 10;
        while (healthCheckLimit > 0 && publisher.getState() != RUNNING) {
            Thread.sleep(1000);
            healthCheckLimit--;
        }
        if (publisher.getState() != RUNNING && publisher.getState() != FINISHED) {
            System.out.println("Publisher couldn't start, something's wrong: state: " + publisher.getState() + " error: " + publisher.getError());
        }
        return publisher;
    }

}
