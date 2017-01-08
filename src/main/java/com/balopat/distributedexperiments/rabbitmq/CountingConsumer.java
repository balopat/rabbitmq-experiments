package com.balopat.distributedexperiments.rabbitmq;

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;


public class CountingConsumer extends ExperimentWorker {


    private static final Logger LOG = LoggerFactory.getLogger(CountingConsumer.class);
    private Map<Integer, Integer> counts = new HashMap<>();
    private Long deadLineToFinish = null;

    public CountingConsumer(ExperimentWorker.ExperimentConfig config, PartitioningExperiment.ExperimentData data) {
        super(config, data, "CountingConsumer");
    }

    public void runUnsafe() throws Throwable {
        setupCounts();

        setupQueue();

        withRetryingConnections(() -> {
            try {
                channel.basicConsume("testqueue", countingConsumerCallback());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

        state = State.RUNNING;

        while (state != State.FINISHED) {
            Thread.sleep(2000);
            printStats();
            if (new Report(counts).countZero == 0) {
                state = State.FINISHED;
            }
            if (hasDeadlineToFinish() && System.currentTimeMillis() > deadLineToFinish) {
                throw new IllegalStateException("Consumer failed to finish on deadline!");
            }
        }
    }

    protected DefaultConsumer countingConsumerCallback() {
        return new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                int key = Integer.parseInt(new String(body));
                counts.put(key, counts.get(key) + 1);
                channel.basicAck(envelope.getDeliveryTag(), false);
                if (key % 20000 == 0 && key / 20000 > 0) {
                    try {
                        int sleepTime = 1000;
                        System.out.println("sleeping for " + sleepTime + "ms...");
                        Thread.sleep(sleepTime);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        };
    }

    private void setupQueue() throws IOException, TimeoutException, InterruptedException {
        withRetryingConnections(() -> {
            try {
                channel.queueDeclare("testqueue", true, false, false, new HashMap<>());
                channel.queueBind("testqueue", "amq.fanout", "");
                channel.queuePurge("testqueue");
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

        LOG.info("Consumer connected successfully.");
    }

    private void setupCounts() {
        for (int i = 0; i < config.sampleSize; i++) {
            counts.put(i, 0);
        }
    }


    private void printStats() {
        System.out.println(new Report(counts).prettyPrint());
    }

    public boolean hasDeadlineToFinish() {
        return this.deadLineToFinish != null;
    }

    public void setDeadLineToFinish(long deadLineToFinish) {
        System.out.println("SET deadline to finish! " + deadLineToFinish + "ms");
        this.deadLineToFinish = System.currentTimeMillis() + deadLineToFinish;
    }

    public static class Report {
        private final long countZero;
        private final long countNonZero;
        private final long countExactlyOne;
        private final long countDupes;
        private final int sampleSize;
        private final Date reportTime;

        public Report(Map<Integer, Integer> counts) {
            sampleSize = counts.size();
            countZero = counts.entrySet().stream().filter(entry -> entry.getValue() == 0).count();
            countNonZero = counts.entrySet().stream().filter(entry -> entry.getValue() > 0).count();
            countExactlyOne = counts.entrySet().stream().filter(entry -> entry.getValue() == 1).count();
            countDupes = counts.entrySet().stream().filter(entry -> entry.getValue() > 1).count();
            reportTime = new Date();
        }

        public String prettyPrint() {
            StringBuffer printOut = new StringBuffer("------- consumer stats  ---------");
            printOut.append("\n");
            printOut.append("time: " + reportTime);
            printOut.append("\n");
            printOut.append("not arrived (0):\t" + countZero + "\t" + (countZero / sampleSize * 100) + "%");
            printOut.append("\n");
            printOut.append("exactly one (=1) :\t" + countExactlyOne + "\t" + (countExactlyOne / sampleSize * 100) + "%");
            printOut.append("\n");
            printOut.append("duplicated (>1):\t" + countDupes + "\t" + (countDupes / sampleSize * 100) + "%");
            printOut.append("\n");
            printOut.append("has arrived:\t" + countNonZero + "\t" + (countNonZero / sampleSize * 100) + "%");
            printOut.append("\n");
            printOut.append("--------------------------------");
            printOut.append("\n");
            return printOut.toString();
        }
    }
}
