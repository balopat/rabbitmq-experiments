package com.balopat.distributedexperiments.rabbitmq;

/**
 * Created by balopat on 1/4/17.
 */
public abstract class ExperimentWorker implements Runnable {

    protected final ExperimentConfig config;
    protected State state = State.INITIALIZING;
    private Throwable error = null;



    public Throwable getError() {
        return error;
    }


    public ExperimentWorker(ExperimentConfig config, PartitioningExperiment.ExperimentData data) {
        this.config = config;
    }


    @Override
    public void run() {
        try {
            runUnsafe();
        } catch (Throwable e) {
            e.printStackTrace();
            state = State.FAILED;
            error = e;
        }
    }

    protected abstract void runUnsafe() throws Throwable;

    public State getState() {
        return state;
    }




    public enum State {
        INITIALIZING, RUNNING, FINISHED, FAILED
    }

    public static class ExperimentConfig {
        protected final int sampleSize;
        protected final long consumerDeadlineAfterPublisherIsDone;

        public ExperimentConfig(int sampleSize, long consumerDeadlineAfterPublisherIsDone) {
            this.sampleSize = sampleSize;
            this.consumerDeadlineAfterPublisherIsDone = consumerDeadlineAfterPublisherIsDone;
        }
    }
}
