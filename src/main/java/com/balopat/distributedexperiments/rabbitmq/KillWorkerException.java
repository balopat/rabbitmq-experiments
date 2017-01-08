package com.balopat.distributedexperiments.rabbitmq;

/**
 * Created by balopat on 1/7/17.
 */
public class KillWorkerException extends RuntimeException {
    public KillWorkerException(String message) {
        super(message);
    }

    public KillWorkerException(String message, Throwable cause) {
        super(message, cause);
    }

    public KillWorkerException(Throwable cause) {
        super(cause);
    }

    protected KillWorkerException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
