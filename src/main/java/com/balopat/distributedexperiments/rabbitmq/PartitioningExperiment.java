package com.balopat.distributedexperiments.rabbitmq;


import com.google.common.collect.Sets;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.balopat.distributedexperiments.rabbitmq.ExperimentWorker.State.*;
import static com.balopat.distributedexperiments.rabbitmq.RabbitMQClusterManager.RABBIT1;
import static com.balopat.distributedexperiments.rabbitmq.RabbitMQClusterManager.RABBIT2;
import static com.balopat.distributedexperiments.rabbitmq.RabbitMQClusterManager.RABBIT3;
import static java.util.Arrays.asList;

public class PartitioningExperiment {

    private static final Logger LOG = LoggerFactory.getLogger(PartitioningExperiment.class);
    public static final String OPT_SAMPLE_SIZE = "sample_size";
    public static final String OPT_CONSUMER_DEADLINE = "consumer_deadline";
    public static final String OPT_NO_PARTITIONING = "no_partitioning";
    public static final String OPT_NO_CLEANUP = "no_cleanup";

    public static void main(String[] args) throws Exception {
        ExperimentWorker.ExperimentConfig config = getExperimentConfig(args);
        LOG.info(config.toString());
        RabbitMQClusterManager rabbitMQClusterManager = new RabbitMQClusterManager();
        try {
            rabbitMQClusterManager.bringUpCluster();
            DockerPartitioner dockerPartitioner = new DockerPartitioner(rabbitMQClusterManager);
            runExperiment(config, dockerPartitioner, rabbitMQClusterManager);
            System.out.println("Experiment finished.");
        } finally {
            if (config.cleanup) {
                rabbitMQClusterManager.cleanup();
            }
        }
    }

    protected static ExperimentWorker.ExperimentConfig getExperimentConfig(String[] args) throws ParseException {
        LOG.info("running with args: " + Arrays.toString(args));

        Options options = new Options();
        options.addOption(OPT_SAMPLE_SIZE, true, "sets the number of messages sent (100,000 default)");
        options.addOption(OPT_NO_CLEANUP, false, "for debugging purposes doesn't cleanup the rabbitmq docker containers");
        options.addOption(OPT_NO_PARTITIONING, false, "good for setting baseline: runs the experiment with no partitioning - no messages should be lost ever in this scenario");
        options.addOption(OPT_CONSUMER_DEADLINE, true, "consumer deadline to finish after the producers finished");


        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);


        long sampleSize = 100000;
        if (cmd.hasOption(OPT_SAMPLE_SIZE)) {
            sampleSize = Long.parseLong(cmd.getOptionValue(OPT_SAMPLE_SIZE));
        }

        boolean cleanup = !cmd.hasOption(OPT_NO_CLEANUP);
        boolean partitioning = !cmd.hasOption(OPT_NO_PARTITIONING);

        long consumerDeadline = 120000;
        if (cmd.hasOption(OPT_CONSUMER_DEADLINE)) {
            consumerDeadline = Long.parseLong(cmd.getOptionValue(OPT_CONSUMER_DEADLINE));
        }


        return new ExperimentWorker.ExperimentConfig(consumerDeadline, sampleSize, cleanup, partitioning);
    }

    private static void runExperiment(ExperimentWorker.ExperimentConfig config, DockerPartitioner dockerPartitioner, RabbitMQClusterManager clusterManager) throws InterruptedException {
        CountingConsumer countingConsumer = setupAndStartConsumer(config);
        waitForHealthyWorker(10, countingConsumer);
        CountingPublisher countingPublisher1 = setupAndStartPublisher(0, config.sampleSize / 2, config);
        CountingPublisher countingPublisher2 = setupAndStartPublisher(config.sampleSize / 2 + 1, config.sampleSize - 1, config);
        waitForHealthyWorker(10, countingPublisher1, countingPublisher2);


        getConsumerCoordinator(countingConsumer).start();
        if (config.partitioning) getNetworkPartitionCoordinator(dockerPartitioner, clusterManager).start();

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
    }


    private static Thread getNetworkPartitionCoordinator(DockerPartitioner dockerPartitioner, RabbitMQClusterManager clusterManager) {
        return new Thread(() -> {

            boolean clusterStateIsValid = clusterManager.assertClusteringState()
                    .from(RABBIT1).clusteredNodesAre(true, true, true)
                    .from(RABBIT2).clusteredNodesAre(true, true, true)
                    .from(RABBIT3).clusteredNodesAre(true, true, true)
                    .validate();

            if (!clusterStateIsValid) {
                throw new RuntimeException("Cluster is not in right state!");
            }


            sleep(10);

            LOG.info("partitioning away rabbit3 from rabbit1 and rabbit2, sleep...");
            dockerPartitioner.partitionAway(RABBIT3, RABBIT1, RABBIT2);
            sleep(20);


            LOG.info("partitioning away rabbit1 from rabbit2 and rabbit3, sleep...");

            dockerPartitioner.partitionAway(RABBIT3, RABBIT1, RABBIT2);
            sleep(20);

            LOG.info("partitioning away rabbit1 from rabbit2 and rabbit3, sleep...");
            dockerPartitioner.partitionAway(RABBIT2, RABBIT1, RABBIT3);
            sleep(20);


            LOG.info("healing rabbit3");
            dockerPartitioner.healNetwork(RABBIT3);

            sleep(10);

            LOG.info("healing rabbit2");
            dockerPartitioner.healNetwork(RABBIT2);
        });
    }

    private static Thread getConsumerCoordinator(CountingConsumer countingConsumer) {
        return new Thread(() -> {

            while (countingConsumer.state != FAILED && countingConsumer.state != FINISHED) {
                LOG.info("waiting 5 sec...");
                sleep(20);
                LOG.info("flipping consumer...");
                if (countingConsumer.state == PAUSED) countingConsumer.resume();
                else if (countingConsumer.state == RUNNING) countingConsumer.pause();
                else LOG.info("stopping - as counting consumer state is: " + countingConsumer.state);
            }

        });
    }

    private static void sleep(int seconds) {
        try {
            Thread.sleep(seconds * 1000);
        } catch (InterruptedException e) {
        }
    }

    private static CountingConsumer setupAndStartConsumer(ExperimentWorker.ExperimentConfig config) throws InterruptedException {
        CountingConsumer consumer = new CountingConsumer(config);
        Thread consumerThread = new Thread(consumer);
        consumerThread.start();
        return consumer;
    }

    private static CountingPublisher setupAndStartPublisher(long startInterval, long endInterval, ExperimentWorker.ExperimentConfig config) throws InterruptedException {
        CountingPublisher publisher = new CountingPublisher(RabbitMQClusterManager.RABBIT2_PORT, startInterval, endInterval, config);
        Thread publisherThread = new Thread(publisher);
        publisherThread.start();
        return publisher;
    }

    private static void waitForHealthyWorker(int healthCheckLimit, ExperimentWorker... workers) throws InterruptedException {
        while (healthCheckLimit > 0 && asList(workers).stream().anyMatch(w -> w.getState() != RUNNING)) {
            Thread.sleep(1000);
            healthCheckLimit--;
        }
        if (asList(workers).stream().anyMatch(w -> w.getState() != RUNNING)
                && asList(workers).stream().anyMatch(w -> w.getState() != FINISHED)) {
            asList(workers).stream().forEach(worker -> System.out.println(worker.getName() +
                    " couldn't start, something's wrong: state: " + worker.getState() +
                    " error: " + worker.getError()));
        }
    }

}
