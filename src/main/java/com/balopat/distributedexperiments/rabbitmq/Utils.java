package com.balopat.distributedexperiments.rabbitmq;

/**
 * Created by balopat on 1/28/17.
 */
public class Utils {
    public static void sleep(int seconds) {
        try {
            Thread.sleep(seconds * 1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
