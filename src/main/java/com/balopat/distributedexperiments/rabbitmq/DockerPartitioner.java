package com.balopat.distributedexperiments.rabbitmq;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.ExecCreateCmdResponse;
import com.github.dockerjava.api.model.Container;
import com.github.dockerjava.api.model.Frame;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.command.ExecStartResultCallback;
import com.sun.media.jfxmedia.logging.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by balopat on 1/4/17.
 */
public class DockerPartitioner {

    public static final String RABBIT1 = "rabbit1";
    public static final String RABBIT2 = "rabbit2";
    public static final String RABBIT3 = "rabbit3";
    private static DockerClientConfig config;
    private static Map<String, RabbitContainer> rabbitContainers = new HashMap<>();
    private static class RabbitContainer {
        String name;
        String ipAddress;
        String containerId;
        private Container container;

        public RabbitContainer(String name, String ipAddress, String containerId, Container container) {
            this.name = name;
            this.ipAddress = ipAddress;
            this.containerId = containerId;
            this.container = container;
        }

        @Override
        public String toString() {
            return "RabbitContainer{" +
                    "name='" + name + '\'' +
                    ", ipAddress='" + ipAddress + '\'' +
                    ", containerId='" + containerId + '\'' +
                    '}';
        }
    }
    private static DockerClient docker;

    public static void main(String[] args) {
        Logger.setLevel(Logger.INFO);
        config = DefaultDockerClientConfig.createDefaultConfigBuilder()
                .build();
        docker = DockerClientBuilder.getInstance(config).build();
        storeContainerInfo(RABBIT1);
        storeContainerInfo(RABBIT2);
        storeContainerInfo(RABBIT3);

        System.out.println(rabbitContainers);

        executeCommand(RABBIT1, "iptables", "-A", "INPUT", "-s", ipOf(RABBIT2), "-j", "DROP");
        executeCommand(RABBIT1, "iptables", "-A", "INPUT", "-s", ipOf(RABBIT3), "-j", "DROP");
        executeCommand(RABBIT1, "iptables", "-L");

    }

    private static String ipOf(String containerName) {
        return rabbitContainers.get(containerName).ipAddress;
    }

    private static void executeCommand(String containerName, String... command) {
        Container container = rabbitContainers.get(containerName).container;
        ExecCreateCmdResponse response = docker
                .execCreateCmd(container.getId())
                .withCmd(command)
                .withAttachStdout(true)
                .withAttachStderr(true)
                .exec();

        try {
            docker.execStartCmd(response.getId())
                    .withDetach(false)
                    .withTty(true)
                    .exec(new ExecStartResultCallback() {
                        @Override
                        public void onNext(Frame frame) {
                            try {
                                System.out.write(frame.getPayload());
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }
                    }).awaitCompletion().close();
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static Container storeContainerInfo(String containerName) {
        Container container = findContainerWithNameContaining(containerName);
        RabbitContainer rabbitContainer = new RabbitContainer(containerName,
                container.getNetworkSettings().getNetworks().get("bridge").getIpAddress(),
                container.getId(),
                container);
        rabbitContainers.put(containerName, rabbitContainer);
        return container;
    }

    private static Container findContainerWithNameContaining(String containerName) {
        return docker.listContainersCmd().exec().stream()
                .filter(container -> container.getNames()[0]
                        .contains(containerName)).findAny()
                .orElseThrow(() -> new IllegalStateException("Please start the rabbitmq containers, " + containerName + " cannot be found."));
    }


}