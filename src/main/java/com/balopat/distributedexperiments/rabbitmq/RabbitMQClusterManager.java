package com.balopat.distributedexperiments.rabbitmq;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.model.*;
import com.github.dockerjava.core.DefaultDockerClientConfig;
import com.github.dockerjava.core.DockerClientBuilder;
import com.github.dockerjava.core.DockerClientConfig;
import com.github.dockerjava.core.command.LogContainerResultCallback;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by balopat on 1/6/17.
 */
public class RabbitMQClusterManager {
    public static final String RABBIT1 = "rabbit1";
    public static final String RABBIT2 = "rabbit2";
    public static final String RABBIT3 = "rabbit3";
    public static int RABBIT1_PORT = 5672;
    public static int RABBIT2_PORT = 5673;
    public static int RABBIT3_PORT = 5674;

    private static final Logger LOG = LoggerFactory.getLogger(RabbitMQClusterManager.class);
    private DockerClientConfig config;
    private DockerClient docker;

    private Map<String, RabbitContainer> rabbitContainers = new HashMap<>();
    public RabbitMQClusterManager() {
        config = DefaultDockerClientConfig.createDefaultConfigBuilder()
                .build();
        docker = DockerClientBuilder.getInstance(config).build();
    }


    public void bringUpCluster() throws InterruptedException, IOException {
        LOG.info("bringing up cluster");
        runRabbitMQClusterManagerContainerWithCmd();

        boolean isClusterUp = false;

        while (!isClusterUp) {
            try {
                HttpURLConnection connection = (HttpURLConnection) new URL("http://localhost:15672/api/aliveness-test/%2F").openConnection();
                String encoded = Base64.getEncoder().encodeToString(("guest:guest").getBytes(StandardCharsets.UTF_8));  //Java 8
                connection.setRequestProperty("Authorization", "Basic " + encoded);
                LOG.info(connection.getResponseCode() + " -> " + connection.getResponseMessage());
                isClusterUp = connection.getResponseCode() / 200 == 1;
            }catch (Exception e) {
                LOG.info("Rabbit cluster is not up yet: " + e + "\n sleeping for 5 seconds...");
                Thread.sleep(5000);
            }
        }

        storeContainerInfo(RABBIT1);
        storeContainerInfo(RABBIT2);
        storeContainerInfo(RABBIT3);

        get(RABBIT1).executeCommand("/opt/rabbit/rabbitmqadmin", "declare", "policy", "name=ha-all", "pattern=testqueue", "definition='{\"ha-mode\n" +
                "\":\"all\"}'");

        LOG.info(rabbitContainers.toString());

    }

    public void cleanup() throws InterruptedException {
        LOG.info("running cleanup");
        runRabbitMQClusterManagerContainerWithCmd("bash", "./cleanup.sh");
    }

    private void runRabbitMQClusterManagerContainerWithCmd(String... cmd) throws InterruptedException {
        Map<String, String> labels = new HashMap<>();
        labels.put("app","rabbitmq-cluster-manager");
        CreateContainerResponse container =  docker.createContainerCmd("balopat/rabbitmq-cluster-manager:latest")
                .withPrivileged(true)
                .withAttachStdout(true)
                .withAttachStderr(true)
                .withBinds(new Bind("/var/run/docker.sock", new Volume("/var/run/docker.sock"), AccessMode.rw))
                .withLabels(labels)
                .withCmd(cmd)
                .exec();

        docker.startContainerCmd(container.getId())
                .exec();


        docker.logContainerCmd(container.getId()).withStdOut(true).withStdErr(true).withFollowStream(true).exec(new LogContainerResultCallback(){
            @Override
            public void onNext(Frame item) {
                LOG.info(item.toString());
            }
        }).awaitCompletion();

        docker.removeContainerCmd(container.getId()).exec();
    }

    public RabbitContainer get(String containerName) {
        return rabbitContainers.get(containerName);
    }



    private Container findContainerWithNameContaining(String containerName) {
        return docker.listContainersCmd().exec().stream()
                .filter(container -> container.getNames()[0]
                        .contains(containerName)).findAny()
                .orElseThrow(() -> new IllegalStateException("Please start the rabbitmq containers, " + containerName + " cannot be found."));
    }
    public String ipOf(String containerName) {
        return rabbitContainers.get(containerName).ipAddress;
    }

    private Container storeContainerInfo(String containerName) {
        Container container = findContainerWithNameContaining(containerName);
        RabbitContainer rabbitContainer = new RabbitContainer(containerName,
                container.getNetworkSettings().getNetworks().get("bridge").getIpAddress(),
                container.getId(),
                container, docker);
        rabbitContainers.put(containerName, rabbitContainer);
        return container;
    }


    public ClusterStateValidatorBuilder assertClusteringState() {
        return new ClusterStateValidatorBuilder(this);
    }
}
